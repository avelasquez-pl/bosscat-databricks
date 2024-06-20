# Databricks notebook source
# MAGIC %md
# MAGIC This notebook contains commonly used functions

# COMMAND ----------

from pyspark.sql import functions as F

def load_table_from_database(connection_config: dict, source_table_name: str):
    df = (
        spark.read.format("postgresql")
        .option("dbtable", source_table_name)
        .option("host", connection_config.get("host"))
        .option("port", connection_config.get("port"))
        .option("database", connection_config.get("database"))
        .option("user", connection_config.get("username"))
        .option("password", connection_config.get("password"))
        .load()
    )
    all_cols = F.concat_ws("", *df.columns)
    df = df.withColumn("hashkey", F.md5(all_cols))
    return df


# COMMAND ----------

def write_table_to_bronze(df, table_name: str):
    dest_table_name = table_name.replace(".", "_")
    dest_table_name = f"bronze.{dest_table_name}"
    df.write.option("mergeSchema", "true").mode("overwrite").saveAsTable(dest_table_name)
