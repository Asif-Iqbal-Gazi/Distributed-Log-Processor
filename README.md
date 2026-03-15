# distributed-log-processor

A distributed log file analytics engine built on Apache Hadoop MapReduce. Processes large-scale log files to compute various statistics such as message type distributions, filtered error counts, and character length analysis — all configurable via a single config file with no hardcoded values.

## Features

- **Task 1** – Distribution of log message types (DEBUG/INFO/WARN/ERROR) across configurable time intervals, filtered by an injected string pattern
- **Task 2** – Count of a specific log level (e.g. ERROR) per time interval sorted in descending order
- **Task 3** – Total count of each log message type across the entire log dataset
- **Task 4** – Maximum character length of pattern-matched strings per log message type

## Prerequisites

- Apache Hadoop 3.3.4
- Java 11
- sbt 1.7.2
- Scala 3.1.3

## Configuration

Before running, update `src/main/resources/application.conf` to match your log format:

```hocon
LogConfiguration {
    # Regex pattern matching the full log line structure
    LogPattern = "(\\d{2}\\:\\d{2}\\:\\d{2}\\.\\d{3})\\s(\\[.+\\])\\s(ERROR|WARN|DEBUG|INFO)\\s+(.*)\\s\\-\\s(.+)"
    # Regex pattern to detect injected strings in the log message body
    InjectedStringPattern = "([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}"
    # Date format used in log timestamps
    DateFormat = "HH:mm:ss.SSS"
    # Time interval size in seconds (e.g. 300 = 5-minute buckets, 3600 = 1-hour buckets)
    Interval = 300
    # Log level to filter on for Task 2
    LogErrorLevel = "ERROR"
}
```

## Build

```bash
sbt clean compile assembly
```

The fat JAR will be created at `target/scala-3.1.3/distributed-log-processor-assembly-0.1.jar`.

## Running on Hadoop

### 1. Start local Hadoop

```bash
start-all.sh
```

### 2. Copy log files to HDFS

```bash
hadoop fs -mkdir -p /user/<YOUR_USERNAME>/input
hadoop fs -put <PATH_TO_LOGFILES> /user/<YOUR_USERNAME>/input
```

### 3. Run a task

```bash
hadoop jar distributed-log-processor-assembly-0.1.jar <TASK_NUMBER> /user/<YOUR_USERNAME>/input /user/<YOUR_USERNAME>/output
```

`TASK_NUMBER` is `1`, `2`, `3`, or `4`.

### 4. View output

```bash
hadoop fs -cat /user/<YOUR_USERNAME>/output/part*
```

### 5. Save output locally

```bash
hadoop fs -cat /user/<YOUR_USERNAME>/output/part* > output.csv
```

## Task Details

### Task 1 — Time Interval Distribution

Outputs the count of each log level per time interval, for log lines whose message body matches the injected string pattern.

```bash
hadoop jar distributed-log-processor-assembly-0.1.jar 1 /user/<YOUR_USERNAME>/input /user/<YOUR_USERNAME>/output
```

Sample output (1-hour intervals):
```
04:00:00 - 04:59:59,DEBUG,3
04:00:00 - 04:59:59,INFO,16
04:00:00 - 04:59:59,WARN,5
07:00:00 - 07:59:59,ERROR,1
07:00:00 - 07:59:59,INFO,41
```

---

### Task 2 — Filtered Log Level Distribution (Descending)

Outputs the count of a specific log level (set via `LogErrorLevel` in config) per time interval, sorted in descending order by count.

```bash
hadoop jar distributed-log-processor-assembly-0.1.jar 2 /user/<YOUR_USERNAME>/input /user/<YOUR_USERNAME>/output
```

Sample output (5-minute intervals, ERROR level):
```
10:15:00 - 10:19:59,16
10:05:00 - 10:09:59,15
10:00:00 - 10:04:59,15
09:55:00 - 09:59:59,13
```

> Note: Task 2 uses two chained MapReduce jobs internally to achieve descending sort.

---

### Task 3 — Total Log Type Counts

Outputs the total count of each log level across all log files.

```bash
hadoop jar distributed-log-processor-assembly-0.1.jar 3 /user/<YOUR_USERNAME>/input /user/<YOUR_USERNAME>/output
```

Sample output:
```
DEBUG,10483
ERROR,1035
INFO,72754
WARN,19829
```

---

### Task 4 — Max Pattern Match Length Per Log Type

Outputs the maximum character length of injected string pattern matches, grouped by log level.

```bash
hadoop jar distributed-log-processor-assembly-0.1.jar 4 /user/<YOUR_USERNAME>/input /user/<YOUR_USERNAME>/output
```

Sample output:
```
DEBUG,45
ERROR,36
INFO,45
WARN,45
```

---

## Project Structure

```
src/
├── main/
│   ├── scala/com/logprocessor/
│   │   ├── MapReduceDriver.scala          # Entry point — dispatches to the right job
│   │   ├── mapper/
│   │   │   ├── TimeIntervalDistributionMapper.scala  # Task 1
│   │   │   ├── FilteredLogLevelMapper.scala          # Task 2 (Part 1)
│   │   │   ├── LogTypeCountMapper.scala              # Task 3
│   │   │   ├── MaxCharLengthMapper.scala             # Task 4
│   │   │   └── SortingMapper.scala                   # Task 2 (Part 2 — sort helper)
│   │   ├── reducer/
│   │   │   ├── SumReducer.scala           # Tasks 1, 2, 3 — sums values per key
│   │   │   ├── MaxReducer.scala           # Task 4 — returns max value per key
│   │   │   └── SortingReducer.scala       # Task 2 — reverses sort swap
│   │   └── utils/
│   │       ├── CreateLogger.scala         # SLF4J logger factory
│   │       └── ComputeIntervals.scala     # Time interval bucketing utility
│   └── resources/
│       ├── application.conf               # All configuration parameters
│       └── logback.xml                    # Logging configuration
└── test/
    └── scala/TestModule/
        └── HelperUtilsTest.scala          # Unit tests
```

## Running Tests

```bash
sbt test
```

## Limitations & Future Work

1. Task 2 currently uses two chained MapReduce jobs to achieve descending sort. A future improvement would achieve the same with a single job.
2. The mapper classes could be unified into a single configurable class using a strategy pattern.
3. Additional error handling for malformed log lines could be added.
