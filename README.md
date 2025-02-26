# Query Optimization in Calcite

*This project was created for the course 15-799: Query Optimization in Database Systems. For more information about the original requirements please see the project [writeup](https://15799.courses.cs.cmu.edu/spring2025/project1.html) for details.*

This simple Java program allows you to explore the effects of different rule sets on the performance of a query optimizer. You can modify the rule set in `ProgramBuilder.java`. 

You will likely need to also change the `Optimizer.java` file to facilitate your specific use case.



## Quickstart

1. Run `./setup.sh` to download DuckDB and the workload.
2. Run `./optimize.sh ./workload.tgz output/` to compile and run the Calcite app.
3. Make your changes to `./calcite_app/src/main/java/edu/cmu/cs/db/calcite_app/app/App.java`.

## Useful Commands

### Gradle

From the `calcite_app/` folder,

```bash
# Build the app into a JAR: ./build/libs/calcite_app-1.0-SNAPSHOT.jar
./gradlew build
# Build the app into a JAR with all dependencies: ./build/libs/calcite_app-1.0-SNAPSHOT-all.jar
./gradlew shadowJar
```

Now, you can invoke your app:

```bash
java -jar build/libs/calcite_app-1.0-SNAPSHOT-all.jar "SELECT 1"
```

You may find it more convenient to chain it together during development:

```bash
./gradlew shadowJar && java -jar build/libs/calcite_app-1.0-SNAPSHOT-all.jar "SELECT 1"
```

If gradle starts acting up, you can clear its cache:

```bash
./gradlew --stop
rm -rf ~/.gradle/caches/ 
```

### Optimizing queries

### Executing queries
You can execute optimized queries in RelRunner and also in DuckDB. RelRunner execution is limited and for example does nots support queries with dates.

The optimizer will output optimized sql plans you can run in duckdb with `PRAGMA disable_optimizer`.