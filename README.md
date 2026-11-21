# Data Engineering

Coursework and practice for big-data processing with Hadoop, Apache Spark and Kafka, organized by unit.

## Contents

| Folder | Topics |
|---|---|
| `Unit-1/` | HDFS commands handbook |
| `Unit-2/` | Spark RDD operations, DataFrame and Spark SQL operations |
| `Unit-3/` | Stream processing: transformation pipelines, tumbling windows, window functions, keyed `ValueState` / `ListState`, word splitting |
| `Unit-4/` | Kafka producers and consumers, Kafka-to-Kafka pipelines |
| `SparkSQL/` | Movie-rating analysis with Spark SQL (scripts and notebooks) |
| `Pyspark-With-Python-main/` | PySpark tutorial notebooks: DataFrames, missing values, filtering, GroupBy and aggregation, PySpark ML and linear regression |

`demo_runner.py` runs the demos.

## Tech stack

Python · PySpark · Spark SQL · Apache Kafka · HDFS · Jupyter

## Running

```bash
pip install pyspark jupyter
jupyter notebook          # for the notebooks
python3 SparkSQL/movie-analysis.py
```

Kafka examples need a running Kafka broker.
