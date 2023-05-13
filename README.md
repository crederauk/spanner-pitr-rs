# Spanner PITR Utility

Enhances [Point In Time Recovery (PITR)](https://cloud.google.com/spanner/docs/pitr) functionality for Google Cloud
Spanner with timestamp detection support.

Current release: 0.1.3

## Description

The objective of this utility is to allow Cloud Spanner users to make use of stale reads and timestamp bounds to
automatically detect the best point in time recovery timestamp in the event of data loss or corruption. It supports
recovering from both DML changes (`INSERT`, `UPDATE` and `DELETE`) as well as DDL statements such as `DROP TABLE`.

### Recovery Window

By default, this point-in-time recovery functionality is limited to the version garbage collection time of [approximately one hour](https://cloud.google.com/spanner/docs/timestamp-bounds#maximum_timestamp_staleness). It is therefore important that in the event of data corruption or deletion, recovery is initiated within this hour window. However, if your database is configured to use the [Spanner PITR functionality](https://cloud.google.com/spanner/docs/pitr) released in March 2021, the recovery window can be extended up to 7 days.

### Recovery Features

This command line utility provides the ability to identify the last point in time before data corruption, using a binary search through the GC timeline with a custom SQL query.

## Running the utility

To build the latest version of the utility into a standalone executable, run `cargo build``. The compiled
binary is then able to be run as a standalone CLI utility, and reasonable defaults are used wherever possible to
minimise the need for configuration. To see the available commands and options, run:

```shell
./spanner-pitr --help
```

When running the tool, ensure that valid credentials for a service account are provided using:

- A JSON file whose path is specified by the GOOGLE_APPLICATION_CREDENTIALS environment variable.
- A JSON file in a location known to the gcloud command-line tool. On Windows, this is %APPDATA%/gcloud/application_default_credentials.json. On other systems, $HOME/.config/gcloud/application_default_credentials.json.
- The metadata server on GCE.

### Detecting pre-corruption timestamps

The utility is designed to search for the latest timestamp at which the specified query returns the boolean value `true`
in the first column of the first record returned. For example, if a table has accidentally been dropped, to find the
latest timestamp at which the check query returns `true` (i.e. before the table was deleted and assuming there was at
least one row of data in it), run the following command:

```shell
./spanner-pitr \
    --project test-project \
    --instance test-instance \
    --database test-db \
    query "SELECT true FROM test_table LIMIT 1" \
    --start 2023-03-01T23:34:43.023443Z # This is optional, to speed up detection time
    --accuracy 10 # This is optional (in ms), to specify the desired accuracy.
```

This approach can be used in the event of data corruption (if records have been deleted or overwritten). However, to
prevent errors in timestamp detection, it is necessary that the query returns `true` for every time period from
the `start` through to the point at which the data is corrupted and false from that point until the `end` timestamp.

Whilst searching the timeline, the utility will output a number of log entries until it finds an appropriate timestamp
within the granularity that has been found.

## Building & testing

Tests can be executed locally using `cargo`, but require a remote Spanner instance to be available (since the emulator
does not yet support this functionality). Prior to running the tests, ensure that the following environment variables
have been set:

```shell
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json;
export SPANNER_INSTANCE=test-instance;
export SPANNER_DATABASE=test-db;
export SPANNER_PROJECT=test-project;
cargo test
```

## Further Reading

- https://cloud.google.com/spanner/docs/pitr
- https://cloud.google.com/spanner
- https://cloud.google.com/spanner/docs/backup
- https://cloud.google.com/logging/docs/view/advanced-queries
- https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances.backups/create
- https://cloud.google.com/dataflow/docs/guides/templates/provided-batch#cloud-spanner-to-cloud-storage-text
- https://cloud.google.com/spanner/docs/reads#read_data_in_parallel
- https://cloud.google.com/sdk/gcloud/reference/spanner/databases/execute-sql#--read-timestamp

## Contributing

Contributions are welcome. Please feel free to raise issues and pull requests as appropriate.

## License

Apache-2.0

## Contact

For any queries, please email [opensource@credera.co.uk](mailto:opensource@credera.co.uk).

Copyright © 2023 Andrew James, Credera UK.
