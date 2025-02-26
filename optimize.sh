#!/usr/bin/env bash

set -euo pipefail

WORKLOAD=$1
OUTPUT_DIR=$2

echo "Invoking optimize.sh."
echo -e "\tWorkload: ${WORKLOAD}"
echo -e "\tOutput Dir: ${OUTPUT_DIR}"

mkdir -p "${OUTPUT_DIR}"
mkdir -p input

# Remove the data.db file if it exists
if [ -f "data.db" ]; then
  rm "data.db"
fi



# Extract the workload.
tar xzf "${WORKLOAD}" -C input

cd calcite_app


# init the database
../duckdb -init ../input/data/schema.sql -no-stdin ../data.db
../duckdb -init ../input/data/load.sql -no-stdin ../data.db



# Build and run the Calcite app.
./gradlew build
./gradlew shadowJar
./gradlew --stop
java -Xmx4096m -jar build/libs/calcite_app-1.0-SNAPSHOT-all.jar "../input/queries" "../${OUTPUT_DIR}"


cd -
