#!/usr/bin/env bash

set -euo pipefail

OUTPUT_DIR=$1


# Feel free to add more steps here.

# launch duckdb and ingest data
# ./duckdb data.db < input/data/schema.sql
# ./duckdb data.db < input/data/load.sql

# Build and run the Calcite app.

rm -rf output/*

cd calcite_app/
./gradlew build
./gradlew shadowJar
./gradlew --stop
java -Xmx4096m -jar build/libs/calcite_app-1.0-SNAPSHOT-all.jar "../input/queries" "../${OUTPUT_DIR}"
cd -

# rm data.db

