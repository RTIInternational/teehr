# Teehr Aggregations — Scala/Spark UDAFs

This directory contains the Scala/Spark implementation of reusable custom aggregation and metric functions (UDAFs) for the teehr project. The library provides JVM-native aggregators for efficient Spark computations.

## Prerequisites

- **Java**: 8 or later (OpenJDK recommended)
- **sbt**: 1.10.1 or later
- **Scala**: 2.13 (managed by sbt)

## Setup

### Install sbt

**macOS (Homebrew):**
```bash
brew install sbt
```

**Linux (apt):**

Requires adding software repository. See [sbt](https://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Linux.html)


```bash
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo -H gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import
sudo chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg
sudo apt-get update
sudo apt-get install sbt
```

**Windows** or manual install:
See https://www.scala-sbt.org/1.x/docs/Setup.html

### Verify Installation

```bash
sbt --version
java -version
```

## Building

### Clean and Build

```bash
cd scala
sbt clean compile
```

### Run Tests

```bash
cd scala
sbt test
```

Run a specific test:
```bash
cd scala
sbt "testOnly com.rti.teehr.aggregations.YourTestName"
```

### Code Formatting

Format all Scala files:
```bash
cd scala
sbt scalafmt
```

Check formatting without changes:
```bash
cd scala
sbt "scalafmt --check"
```

### Create a JAR

To build a JAR for use in Spark jobs:
```bash
cd scala
sbt package
```

JAR output: `scala/target/scala-2.13/teehr-aggregations_2.13-0.1.0.jar`

To create a fat JAR (includes assembly), you'll need to add the `sbt-assembly` plugin. See the "Future: Fat JAR" section below.

## Project Structure

```
scala/
├── build.sbt                    # Build configuration
├── .scalafmt.conf              # Scala code formatter config
├── project/
│   └── build.properties         # sbt version pinning
├── src/
│   ├── main/scala/
│   │   └── com/rti/teehr/aggregations/
│   │       └── (UDAF implementations)
│   └── test/scala/
│       └── com/rti/teehr/aggregations/
│           └── (ScalaTest suites)
└── target/                      # Build output (generated)
```

## Adding a New UDAF

1. Create a Scala file in `src/main/scala/com/rti/teehr/aggregations/` with your aggregation logic.
2. Extend or implement the appropriate Spark aggregator interface.
3. Add a corresponding test file in `src/test/scala/com/rti/teehr/aggregations/`.
4. Run `sbt test` to validate.

See existing implementations for patterns and null-safety best practices.

## Key Dependencies

- **Spark SQL 4.0.1** (provided): Core aggregation and SQL infrastructure
- **ScalaTest 3.2.18** (test): Unit test framework

## Troubleshooting

### sbt Download Issue
If sbt takes a long time on first run, it's downloading the Scala compiler and libraries. This is normal.

### Java Version Mismatch
Ensure your `java -version` output matches the project requirement (Java 8+). You may need to set `JAVA_HOME`:
```bash
export JAVA_HOME=/path/to/java
```

### Spark Version Mismatch
This library is built against Spark 4.0.1 (compiled for Scala 2.13). If you're using a different Spark version in your job, you may encounter classpath issues. Verify Spark version compatibility before using the JAR.

## Future: Fat JAR

To enable assembly and create a fat JAR:

1. Add to `project/plugins.sbt`:
   ```scala
   addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.3")
   ```

2. Build fat JAR:
   ```bash
   sbt assembly
   ```

Output: `scala/target/scala-2.13/teehr-aggregations-assembly-0.1.0.jar`

## Next Steps

- Implement the first UDAF (see step 3 in the plan).
- Write unit tests to validate behavior.
- Integrate JAR into Python tests (optional, deferred for now).

## Questions?

See the parent repository README and the plan document for more context.
