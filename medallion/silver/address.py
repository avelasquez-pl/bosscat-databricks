# Databricks notebook source
# MAGIC %sql
# MAGIC create table if not exists silver.address (
# MAGIC   id string,
# MAGIC   city string,
# MAGIC   country string,
# MAGIC   county string,
# MAGIC   latitude double,
# MAGIC   line1 string,
# MAGIC   line2 string,
# MAGIC   longitude double,
# MAGIC   state string,
# MAGIC   time_zone string,
# MAGIC   zip_code string,
# MAGIC   hashkey string
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into silver.address s
# MAGIC using bronze.app_address b
# MAGIC on s.id = b.id
# MAGIC when matched and s.hashkey != b.hashkey then update set *
# MAGIC when not matched then insert *
# MAGIC when not matched by source then delete
