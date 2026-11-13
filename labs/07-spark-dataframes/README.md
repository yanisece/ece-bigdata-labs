# Lab 07 - Spark DataFrames and SQL

This lab implements the Bronze-to-Silver pipeline with Spark DataFrames and Spark SQL.

## Local validation

```bash
spark-submit --master 'local[*]' labs/07-spark-dataframes/lab_silver.py --mode local
```

## S3 execution on Onyxia

Refresh the AWS credentials in the terminal, then run:

```bash
spark-submit --master 'local[*]' labs/07-spark-dataframes/lab_silver.py --mode s3
```

The script reads the Bronze CSV datasets, applies schema validation, deduplication and window transformations, then writes the cleaned users and orders datasets as Parquet files in the Silver layer.
