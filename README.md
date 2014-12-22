copytables
==========

Library and tool to copy tables from one database to another.

## Prerequisites

    + Ubuntu (preferably 12.04)

    + GHC 7.6.3 or later

    + Cabal 1.19.2 or later

    + Happy 1.19 or later

## Clone and build:

    Clone the repo:
    $ git clone https://github.com/zalora/copytables.git

    Build with Docker (remember to configure your credentials first in ./config folder)
    $ cd copytables/
    $ sudo docker build -t="copytables" .

## Executables (to run with Docker: use `docker run copytables` as a prefix for the below commands)

    Show help
    .cabal-sandbox/bin/copytables --help

    Copy table
    .cabal-sandbox/bin/copytables --from source_db --from-table schema_name.table_name --to destination_db --to-table schema_name.table_name [--cascade]
