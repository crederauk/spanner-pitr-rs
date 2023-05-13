use std::fmt::Display;
use std::num::ParseIntError;
use std::ops::Deref;

use anyhow::{anyhow, Result};
use async_recursion::async_recursion;
use clap::{arg, Parser, Subcommand};
use google_cloud_default::WithAuthExt;
use google_cloud_googleapis::spanner::admin::database::v1::GetDatabaseRequest;
use google_cloud_spanner::admin::client::Client as AdminClient;
use google_cloud_spanner::admin::AdminClientConfig;
use google_cloud_spanner::client::{Client, ClientConfig};
use google_cloud_spanner::reader::AsyncIterator;
use google_cloud_spanner::statement::Statement;
use google_cloud_spanner::value::{Timestamp, TimestampBound};
use indicatif::ProgressBar;

use log::{debug, error, info, trace, warn};
use time::{error::Parse, ext::NumericalDuration, OffsetDateTime};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Arguments {
    /// Google Cloud Project
    #[arg(short, long)]
    project: String,
    /// Cloud Spanner Instance
    #[arg(short, long)]
    instance: String,
    /// Cloud Spanner Database
    #[arg(short, long)]
    database: String,

    /// Debug mode
    #[arg(long, action = clap::ArgAction::Count, default_value_t=0)]
    debug: u8,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Query {
        /// Spanner diagnostic query
        #[arg(short, long)]
        query: String,
        /// Beginning of query window (optional)
        #[arg(short, long, value_parser=parse_timestamp)]
        start: Option<OffsetDateTime>,
        /// End of query window (optional)
        #[arg(short, long, value_parser=parse_timestamp)]
        end: Option<OffsetDateTime>,
        /// Granularity
        #[arg(short, long, value_parser=parse_duration, default_value_t=DisplayableDuration(10.milliseconds()))]
        accuracy: DisplayableDuration,
    },
}

#[derive(Debug, Clone, Copy)]
struct DisplayableDuration(time::Duration);

impl Display for DisplayableDuration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self.0.whole_milliseconds()))
    }
}

impl Deref for DisplayableDuration {
    type Target = time::Duration;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

trait ToOffsetDateTime {
    fn to_offset_date_time(&self) -> OffsetDateTime;
}

impl ToOffsetDateTime for google_cloud_spanner::value::Timestamp {
    fn to_offset_date_time(&self) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(self.seconds)
            .unwrap()
            .replace_nanosecond(self.nanos as u32)
            .unwrap()
    }
}

impl ToOffsetDateTime for prost_types::Timestamp {
    fn to_offset_date_time(&self) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(self.seconds)
            .unwrap()
            .replace_nanosecond(self.nanos as u32)
            .unwrap()
    }
}

/// Logic to find the closest timestamp at which the check query returns `true` in the
/// first column of the first row.
struct TimestampFinder {
    start: OffsetDateTime,
    end: OffsetDateTime,
    accuracy: time::Duration,
    query: String,
    client: Client,
}

impl TimestampFinder {
    /// Run a Spanner query at a specific timestamp.
    async fn query_at(&self, ts: &OffsetDateTime) -> Result<bool> {
        let mut tx = self
            .client
            .single_with_timestamp_bound(TimestampBound::read_timestamp(Timestamp {
                seconds: ts.unix_timestamp(),
                nanos: ts.nanosecond() as i32,
            }))
            .await?;

        match tx.query(Statement::new(&self.query)).await {
            Ok(mut rows) => match rows.next().await {
                Ok(Some(row)) => row
                    .column::<bool>(0)
                    .map_err(|e| anyhow!(format!("column error: {e}"))),
                Ok(None) => Ok(false),
                Err(status) if status.message().contains("Table not found") => Ok(false),
                Err(status)
                    if status
                        .message()
                        .contains("exceeded the maximum timestamp staleness") =>
                {
                    warn!("{}", status.message());
                    Ok(false)
                }
                Err(status) => Err(status.into()),
            },
            // Don't treat a table not being found as a fatal error. Often required when
            // recovering from DDL errors, such as dropping tables.
            Err(status) if status.message().contains("Table not found") => Ok(false),
            // Treat this as a soft error and continue processing.
            Err(status)
                if status
                    .message()
                    .contains("exceeded the maximum timestamp staleness") =>
            {
                warn!("{}", status.message());
                Ok(false)
            }
            Err(status) => Err(status.into()),
        }
    }

    /// Find the latest timestamp at which the database query returns `true`
    /// in the first column of the first row.
    #[async_recursion]
    async fn find_timestamp<I>(
        &self,
        start: &OffsetDateTime,
        end: &OffsetDateTime,
        remaining_iterations: u32,
        increment_progress: I,
    ) -> Result<OffsetDateTime>
    where
        I: std::marker::Send
            + Fn(OffsetDateTime, OffsetDateTime, OffsetDateTime, bool) -> Result<()>,
    {
        let midpoint = Self::timestamp_midpoint(start, end);
        debug!("Querying between {} and {} at {}...", start, end, midpoint);

        // Error if the search interval is exhausted (or start >= end)
        if (*end - *start) <= 0.seconds() {
            return Err(anyhow!(
                "Maximum accuracy reached without finding a result."
            ));
        }

        // Error if there are no more iterations
        if remaining_iterations == 0 {
            return Err(anyhow!(
                "Exhausted expected iterations without finding a result."
            ));
        }

        let result = match self.query_at(&midpoint).await {
            Ok(true) => {
                increment_progress(*start, *end, midpoint, true)?;
                if (*end - midpoint) < self.accuracy {
                    // Successfully found a timestamp within the accuracy interval.
                    trace!("  Query succeeded. Closest timestamp found: {}", midpoint);
                    Ok(midpoint)
                } else {
                    // Query succeeded, but not yet accurate enough. Search later.
                    trace!("  Query succeeded (not within accuracy window). Searching later.");
                    self.find_timestamp(
                        &midpoint,
                        end,
                        remaining_iterations - 1,
                        increment_progress,
                    )
                    .await
                }
            }
            Ok(false) => {
                // Query failed. Search earlier.
                trace!("  Query failed. Searching earlier.");
                increment_progress(*start, *end, midpoint, false)?;
                self.find_timestamp(
                    start,
                    &midpoint,
                    remaining_iterations - 1,
                    increment_progress,
                )
                .await
            }
            Err(e) => {
                // Log error and search earlier.
                error!("  Query failed ({}). Searching earlier.", e);
                self.find_timestamp(
                    start,
                    &midpoint,
                    remaining_iterations - 1,
                    increment_progress,
                )
                .await
            }
        };

        match result {
            Ok(ts) => Ok(ts),
            Err(e) => Err(e),
        }
    }

    /// Check that the query returns `true` at the beginning of the period and
    /// `false` at the end of the period.
    async fn check_bounds(&self) -> Result<()> {
        if self.query_at(&self.end).await? {
            return Err(anyhow!(
                "Check query returned `true` at the end of the time window."
            ));
        }

        if !self.query_at(&self.start).await? {
            return Err(anyhow!(
                "Check query returned `false` at the start of the time window."
            ));
        }

        Ok(())
    }

    /// Execute the timestamp finder.
    async fn run(&self) -> Result<OffsetDateTime> {
        info!(
            "❔ Checking query at start ({}) and end ({}) timestamps...",
            self.start, self.end
        );

        self.check_bounds().await?;

        let bar = ProgressBar::new(self.expected_queries().into());
        info!("❔ Searching for closest recovery timestamp...");
        let ts = self
            .find_timestamp(
                &self.start,
                &self.end,
                self.expected_queries(),
                |start, end, mp, res| {
                    bar.set_message(format!("{} - {} - {} ({})", start, mp, end, res));
                    bar.inc(1);
                    Ok(())
                },
            )
            .await;

        bar.finish();
        ts
    }

    /// Calculate the number of timeline checks expected.
    fn expected_queries(&self) -> u32 {
        ((self.end - self.start).whole_nanoseconds() / self.accuracy.whole_nanoseconds()).ilog2()
            + 2
    }

    // Calculate the mid-point of two timestamps.
    fn timestamp_midpoint(start: &OffsetDateTime, end: &OffsetDateTime) -> OffsetDateTime {
        start.saturating_add((*end - *start) / 2)
    }
}

/// Parse a timestamp from an RFC3339-formatted string.
fn parse_timestamp(ts: &str) -> Result<OffsetDateTime, Parse> {
    OffsetDateTime::parse(ts, &time::format_description::well_known::Rfc3339)
}

/// Parse a duration from a number of milliseconds.
fn parse_duration(millis: &str) -> Result<DisplayableDuration, ParseIntError> {
    Ok(DisplayableDuration(time::Duration::milliseconds(
        millis.parse::<i64>()?,
    )))
}

/// Return the current time of the database server.
async fn database_time(client: &Client) -> Result<OffsetDateTime> {
    let mut tx = client.single().await?;

    let mut rows = tx
        .query(Statement::new("SELECT CURRENT_TIMESTAMP()"))
        .await?;

    if let Some(row) = rows.next().await? {
        row.column::<OffsetDateTime>(0)
            .map_err(|e| anyhow!(format!("column error: {e}")))
    } else {
        Err(anyhow!("Could not return commit timestamp."))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Arguments::parse();

    // Configure logger from command line parameters.
    env_logger::Builder::new()
        .filter_level(match args.debug {
            0 => log::LevelFilter::Info,
            1 => log::LevelFilter::Debug,
            2.. => log::LevelFilter::Trace,
        })
        .init();

    // Connect to database.
    let cfg = ClientConfig::default().with_auth().await?;
    let admin_cfg = AdminClientConfig::default().with_auth().await?;
    let admin_client = AdminClient::new(admin_cfg).await?;

    let database = format!(
        "projects/{}/instances/{}/databases/{}",
        args.project, args.instance, args.database
    );
    info!("ℹ️ Connecting to database: {}", database);

    match args.command {
        Command::Query {
            query,
            start,
            end,
            accuracy,
        } => {
            let database_info = admin_client
                .database()
                .get_database(
                    GetDatabaseRequest {
                        name: database.clone(),
                    },
                    None,
                )
                .await?
                .into_inner();
            let earliest_time = &database_info
                .earliest_version_time
                .unwrap()
                .to_offset_date_time();

            let retention_period = &database_info.version_retention_period;

            info!("⏱️ Earliest recovery time: {}", &earliest_time);
            info!("⏱️ Retention period: {}", &retention_period);
            let client = Client::new(database.clone(), cfg).await?;
            let database_time = database_time(&client).await?;

            let finder = TimestampFinder {
                start: start.unwrap_or(*earliest_time),
                end: end.unwrap_or(database_time),
                accuracy: *accuracy,
                query,
                client,
            };

            let target = finder.run().await?;
            info!("✅ Found closest recovery timestamp: {}", target);
            info!("ℹ️ To back up a database at this point in time:");
            info!("ℹ️   gcloud spanner backups create {} --instance={} --database={} --expiration-date={} --async",
                    format!("backup-{}", uuid::Uuid::new_v4().simple()), &args.instance, &args.database, &target.format(&time::format_description::well_known::Rfc3339)?);
            info!("ℹ️ To execute a query at this point in time:");
            info!("ℹ️   gcloud spanner databases execute-sql {} --project={} --instance={} --sql='SELECT true' --read-timestamp={}", &args.database, &args.project, &args.instance,
                    &target.format(&time::format_description::well_known::Rfc3339)?);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use anyhow::{anyhow, Ok, Result};

    use google_cloud_default::WithAuthExt;
    use google_cloud_googleapis::spanner::admin::database::v1::UpdateDatabaseDdlRequest;
    use google_cloud_spanner::{
        admin::{client::Client as AdminClient, AdminClientConfig},
        client::{Client, ClientConfig},
        reader::AsyncIterator,
        statement::Statement,
    };
    use log::info;
    use std::{env, time::Duration};
    use time::OffsetDateTime;

    use crate::{TimestampFinder, ToOffsetDateTime};

    struct TestSpanner {
        project: String,
        instance: String,
        database: String,
    }

    impl TestSpanner {
        fn new() -> Result<TestSpanner> {
            Ok(TestSpanner {
                project: env::var("SPANNER_PROJECT")?,
                instance: (env::var("SPANNER_INSTANCE")?),
                database: env::var("SPANNER_DATABASE")?,
            })
        }

        /// Return the full database path.
        fn database_path(&self) -> String {
            format!(
                "projects/{}/instances/{}/databases/{}",
                self.project, self.instance, self.database
            )
        }

        /// Connect to the specified Spanner database.
        async fn connect(&self) -> Result<(Client, AdminClient)> {
            let cfg = ClientConfig::default().with_auth().await?;
            let admin_cfg = AdminClientConfig::default().with_auth().await?;

            Ok((
                Client::new(self.database_path(), cfg).await?,
                AdminClient::new(admin_cfg).await?,
            ))
        }
    }

    /// Test Spanner connectivity and simple queries.
    #[tokio::test]
    async fn test_connectivity() -> Result<()> {
        let spanner = TestSpanner::new()?;
        let (client, _admin_client) = spanner.connect().await?;

        let mut tx = client.single().await?;
        let mut rows = tx.query(Statement::new("SELECT 1")).await?;
        while let Some(row) = rows.next().await? {
            assert!(row.column::<i64>(0)? == 1);
        }

        client.close().await;

        Ok(())
    }

    /// Create a named test table in the database.
    async fn create_test_table(client: &AdminClient, database: &str, name: &str) -> Result<()> {
        let mut metadata = client
            .database()
            .update_database_ddl(
                UpdateDatabaseDdlRequest {
                    database: database.to_string(),
                    statements: vec![format!(
                        r#"CREATE TABLE {} (
                            id STRING(40),
                            committed TIMESTAMP NOT NULL OPTIONS (
                            allow_commit_timestamp = true
                        ),
                  ) PRIMARY KEY(id)"#,
                        name
                    )],
                    operation_id: "".to_string(),
                },
                None,
            )
            .await?;

        metadata.wait(None).await?;

        Ok(())
    }

    /// Drop the named test table.
    async fn drop_test_table(client: &AdminClient, database: &str, name: &str) -> Result<()> {
        let mut metadata = client
            .database()
            .update_database_ddl(
                UpdateDatabaseDdlRequest {
                    database: database.to_string(),
                    statements: vec![format!(r#"DROP TABLE {}"#, name)],
                    operation_id: "".to_string(),
                },
                None,
            )
            .await?;

        metadata.wait(None).await?;

        Ok(())
    }

    /// Return the current time of the database server.
    async fn database_time(client: &Client) -> Result<OffsetDateTime> {
        let mut tx = client.single().await?;

        let mut rows = tx
            .query(Statement::new("SELECT CURRENT_TIMESTAMP()"))
            .await?;

        if let Some(row) = rows.next().await? {
            row.column::<OffsetDateTime>(0)
                .map_err(|e| anyhow!(format!("column error: {e}")))
        } else {
            Err(anyhow!("Could not return commit timestamp."))
        }
    }

    /// Insert a record into the specified test table.
    async fn insert_test_record(
        client: &Client,
        table_name: &str,
    ) -> Result<(OffsetDateTime, String)> {
        // Create a table and insert some data.
        let insert_id = uuid::Uuid::new_v4().to_string();
        let mut tx = client.begin_read_write_transaction().await?;

        let insert_queries = async {
            let mut insert_query = Statement::new(format!(
                "INSERT INTO {}(id, committed) VALUES (@id, PENDING_COMMIT_TIMESTAMP())",
                table_name
            ));
            insert_query.add_param("id", &insert_id);
            tx.update(insert_query).await
        }
        .await;

        let insert_result = tx.end(insert_queries, None).await?;

        Ok((insert_result.0.unwrap().to_offset_date_time(), insert_id))
    }

    /// Test recovery from DML changes
    #[tokio::test]
    async fn test_dml_insertion_recovery() -> Result<()> {
        let spanner = TestSpanner::new()?;
        let (client, admin_client) = spanner.connect().await?;
        let test_table = format!("table_{}", uuid::Uuid::new_v4().simple());

        let operation = async {
            create_test_table(&admin_client, &spanner.database_path(), &test_table).await?;

            let (insert_timestamp, _) = insert_test_record(&client, &test_table).await?;

            tokio::time::sleep(Duration::from_millis(1000)).await;

            // Delete the rows
            let mut tx = client.begin_read_write_transaction().await?;
            let delete_queries = async {
                let query = Statement::new(format!("DELETE FROM {} WHERE 1=1", test_table));
                tx.update(query).await
            }
            .await;
            let (delete_timestamp, _) = tx.end(delete_queries, None).await?;
            let target_timestamp = delete_timestamp.unwrap().to_offset_date_time();

            tokio::time::sleep(Duration::from_millis(1000)).await;

            let target_accuracy = time::Duration::milliseconds(10);

            // Try to find the correct insertion timestamp.
            let finder = TimestampFinder {
                start: insert_timestamp,
                end: database_time(&client).await?,
                accuracy: target_accuracy,
                query: format!("SELECT COUNT(*) > 0 FROM {}", test_table),
                client,
            };

            let found_timestamp = finder.run().await?;

            if found_timestamp <= target_timestamp
                && (target_timestamp - found_timestamp) < target_accuracy
            {
                Ok(())
            } else {
                Err(anyhow!(
                    "Found timestamp {} and target timestamp {}",
                    found_timestamp,
                    target_timestamp
                ))
            }
        }
        .await;

        // Always drop the test table afterwards.
        drop_test_table(&admin_client, &spanner.database_path(), &test_table).await?;

        operation
    }

    /// Test recovery from DDL changes
    #[tokio::test]
    async fn test_ddl_recovery() -> Result<()> {
        let spanner = TestSpanner::new()?;
        let (client, admin_client) = spanner.connect().await?;
        let test_table = format!("table_{}", uuid::Uuid::new_v4().simple());

        async {
            let target_accuracy = time::Duration::milliseconds(50);

            create_test_table(&admin_client, &spanner.database_path(), &test_table).await?;
            let start_timestamp = insert_test_record(&client, &test_table).await?.0;

            // Delete the rows
            let mut tx = client.begin_read_write_transaction().await?;
            let delete_queries = async {
                let query = Statement::new(format!("DELETE FROM {} WHERE 1=1", test_table));
                tx.update(query).await
            }
            .await;
            let (delete_timestamp, _) = tx.end(delete_queries, None).await?;
            let target_timestamp = delete_timestamp.unwrap().to_offset_date_time();

            tokio::time::sleep(Duration::from_millis(2000)).await;

            drop_test_table(&admin_client, &spanner.database_path(), &test_table).await?;

            // Try to find the correct insertion timestamp after the table has been dropped.
            let finder = TimestampFinder {
                start: start_timestamp,
                end: database_time(&client).await?,
                accuracy: target_accuracy,
                query: format!("SELECT COUNT(*) > 0 FROM {}", test_table),
                client,
            };

            let found_timestamp = finder.run().await?;

            if found_timestamp <= target_timestamp
                && (target_timestamp - found_timestamp) <= target_accuracy
            {
                info!(
                    "✔️ Found timestamp {} within accuracy of expected timestamp {}",
                    found_timestamp, target_timestamp
                );
                Ok(())
            } else {
                Err(anyhow!(
                    "❌ Found timestamp {}, but  expected timestamp {}.",
                    found_timestamp,
                    target_timestamp
                ))
            }
        }
        .await
    }
}
