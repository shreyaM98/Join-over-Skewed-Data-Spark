# Join-over-Skewed-Data-Spark

### Big Data Management

This project uses Scala version: 2.3.7 and spark-sql version: 3.2.0

#### Problem statement:
Problem is to join two large datasets when one of them is Skewed. We used Customer and Transaction dataset.Transaction dataset is skewed on few customer keys.
We know that dataset is skewed but which keys are skewed is not known.

Naive Approach: Repartition Join

Smart Approach reference link: https://cwiki.apache.org/confluence/display/Hive/Skewed+Join+Optimization
