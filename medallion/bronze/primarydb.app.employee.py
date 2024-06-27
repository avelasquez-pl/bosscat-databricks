# Databricks notebook source
# MAGIC %run "../../utils/utils"

# COMMAND ----------

connection_config = {
    "host": dbutils.secrets.get(scope="primarydb", key="host"),
    "port": dbutils.secrets.get(scope="primarydb", key="port"),
    "database": dbutils.secrets.get(scope="primarydb", key="database"),
    "username": dbutils.secrets.get(scope="primarydb", key="username"),
    "password": dbutils.secrets.get(scope="primarydb", key="password"),
}

# COMMAND ----------

table_name = "app.employee"

# COMMAND ----------

remote_table = load_table_from_database(connection_config, table_name)

# COMMAND ----------

write_table_to_bronze(remote_table, table_name)
