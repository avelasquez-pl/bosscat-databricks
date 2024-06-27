# Databricks notebook source
# MAGIC %md
# MAGIC This notebook contains commonly used functions

# COMMAND ----------

# imports
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

def load_table_from_database(connection_config: dict, source_table_name: str):
    return (
        spark.read.format("postgresql")
        .option("dbtable", source_table_name)
        .option("host", connection_config.get("host"))
        .option("port", connection_config.get("port"))
        .option("database", connection_config.get("database"))
        .option("user", connection_config.get("username"))
        .option("password", connection_config.get("password"))
        .load()
    )

# COMMAND ----------

def load_table_from_database_jdbc(connection_config: dict, source_table_name: str, partition_column: str, lower_bound, upper_bound):
    driver = "org.postgresql.Driver"
    url = f"jdbc:postgresql://{connection_config.get('host')}:{connection_config.get('port')}/{connection_config.get('database')}"
    return (
        spark.read.format("jdbc")
        .option("driver", driver)
        .option("url", url)
        .option("dbtable", source_table_name)
        .option("user", connection_config.get("username"))
        .option("password", connection_config.get("password"))
        .option("partitionColumn", partition_column)
        .option("lowerBound", lower_bound)
        .option("upperBound", upper_bound)
        .option("numPartitions", 8)
        .load()
    )


# COMMAND ----------

def write_table_to_bronze(df, table_name: str):
    dest_table_name = table_name.replace(".", "_")
    dest_table_name = f"bronze.{dest_table_name}"
    df.write.option("mergeSchema", "true").mode("overwrite").saveAsTable(dest_table_name)

# COMMAND ----------

def scd2(table_name: str):
    source = f"silver.{table_name}"
    df_source = spark.read.table(source)
    all_cols = F.concat_ws("", *df_source.columns)
    df_source = df_source.withColumn("surrogate_key", F.md5(all_cols))
    df_source = df_source.withColumn("scd_inserted_on", F.current_timestamp())
    df_source.createTempView("source")

    target = f"silver.{table_name}_scd"

    if spark.catalog.tableExists(target):
        dt_target = DeltaTable.forName(spark, target)
        (
            dt_target.alias("scd")
                .merge(
                    df_source.alias("source"),
                    "scd.id = source.id and scd.surrogate_key = source.surrogate_key")
                .whenNotMatchedInsertAll()
                .execute()
        )

    else:
        df_source.write.option("mergeSchema", "true").mode("overwrite").saveAsTable(target)
