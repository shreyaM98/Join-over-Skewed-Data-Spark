# Join-over-Skewed-Data-Spark

This project uses Scala version: and Spark version:

Join two large datasets when one of them is Skewed

We used Customer and Transaction dataset

Transaction dataset is skewed on few customer keys

We know that dataset is skewed but which keys are skewed is not known.

Naive Approach: Repartition Join

Smart Approach reference link: https://cwiki.apache.org/confluence/display/Hive/Skewed+Join+Optimization
