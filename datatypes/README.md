# Datatypes
Query engines need ways to represent the different data types they will process. You can invent your own or use the 
type system of the queried data source. Engines like Apache Spark actually do both: It has data types like 
StringType, IntegerType, among others, and can infer the schema from data sources like Parquet.

For this project, Apache Arrow is the foundation for our type system.

# Why Arrow?

Apache Arrow is an in-memory **columnar format** that aims to be a universal analytical format for data tooling and **programming
languages.**

## Row-based vs. Columnar
For large-scale analytical queries, processing data types in a columnar format is more effective.
This approach provides several advantages: improved compression from data locality and enhanced processing speed through 
vectorization, utilizing modern SIMD CPUs.

Check this link out: [Why is ClickHouse so fast?](https://clickhouse.com/docs/en/concepts/why-clickhouse-is-so-fast)

Most query engines are based on the [Volcano Query Planner](https://paperhub.s3.amazonaws.com/dace52a42c07f7f8348b08dc2b186061.pdf),
where each step of the physical plan involves iterating one row at a time. This approach, however, adds overhead when running 
queries against billions of rows. Iterating over batches can reduce this overhead. Furthermore, by iterating over batches of columns, rather than
rows, you can vectorize the processing as mentioned earlier.

## Language Interoperability
We also want the query engine to be available in multiple languages. Most popular engines are available in common languages: 
Java, Scala, Python, R, among others.
