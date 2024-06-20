# Databricks notebook source
# MAGIC %sql
# MAGIC create table if not exists silver.email (
# MAGIC   id string,
# MAGIC   description string,
# MAGIC   email string,
# MAGIC   email_type string,
# MAGIC   name string,
# MAGIC   hashkey string
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into silver.email s
# MAGIC using bronze.app_email b
# MAGIC on s.id = b.id
# MAGIC when matched and s.hashkey != b.hashkey then update set *
# MAGIC when not matched then insert *
# MAGIC when not matched by source then delete
