# Databricks notebook source
# MAGIC %run "/Workspace/Users/andres.velasquez@bosscathome.com/utils/utils"

# COMMAND ----------

connection_config = {
    "host": dbutils.secrets.get(scope="devprimarydb", key="host"),
    "port": dbutils.secrets.get(scope="devprimarydb", key="port"),
    "database": dbutils.secrets.get(scope="devprimarydb", key="database"),
    "username": dbutils.secrets.get(scope="devprimarydb", key="username"),
    "password": dbutils.secrets.get(scope="devprimarydb", key="password"),
}

# COMMAND ----------

table_name = "app.item"

# COMMAND ----------

remote_table = load_table_from_database(connection_config, table_name)

# COMMAND ----------

write_table_to_bronze(remote_table, table_name)
