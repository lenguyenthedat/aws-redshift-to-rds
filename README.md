aws-redshift-to-rds
==========

A tool to copy tables from Amazon Redshift to Amazon RDS (PostgreSQL).

This tool should also work as long as the upstream database is PostgreSQL 8.0+ and the downstream database is PostgreSQL 9.0+ (where DBLink is supported).

## Prerequisites

    + Ubuntu (preferably 14.04)

    + GHC 7.8.3 or later

    + Cabal 1.20.0.2 or later

    + Happy 1.19 or later

## Clone and build:

    Clone the repo:
    $ git clone https://github.com/lenguyenthedat/aws-redshift-to-rds.git

    Build with Docker (remember to configure your credentials first in ./config folder)
    $ cd aws-redshift-to-rds/
    $ sudo docker build -t="aws-redshift-to-rds" .

## Executables

(to run with Docker: use `docker run aws-redshift-to-rds` as a prefix for the below commands)

    Show help
    .cabal-sandbox/bin/aws-redshift-to-rds --help

    Copy table
    .cabal-sandbox/bin/aws-redshift-to-rds --from source_db --from-table schema_name.table_name --to destination_db --to-table schema_name.table_name [--cascade]
