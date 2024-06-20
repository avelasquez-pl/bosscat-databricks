# Databricks notebook source
# MAGIC %sql
# MAGIC create table if not exists silver.estimate_contact (
# MAGIC   id string,
# MAGIC   approval_type string,
# MAGIC   approved_on timestamp,
# MAGIC   contact_id string,
# MAGIC   email string,
# MAGIC   first_name string,
# MAGIC   is_initial_authorizer boolean,
# MAGIC   is_payer boolean,
# MAGIC   is_requester boolean,
# MAGIC   is_scheduler boolean,
# MAGIC   last_name string,
# MAGIC   other_role string,
# MAGIC   phone string,
# MAGIC   role string,
# MAGIC   send_estimate boolean,
# MAGIC   hashkey string
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into silver.estimate_contact s
# MAGIC using bronze.app_estimate_contact b
# MAGIC on s.id = b.id
# MAGIC when matched and s.hashkey != b.hashkey then update set 
# MAGIC   s.id = b.id,
# MAGIC   s.approval_type = b.approval_type,
# MAGIC   s.approved_on = from_unixtime(b.approved_on),
# MAGIC   s.contact_id = b.contact_id,
# MAGIC   s.email = b.email,
# MAGIC   s.first_name = b.first_name,
# MAGIC   s.is_initial_authorizer = b.is_initial_authorizer,
# MAGIC   s.is_payer = b.is_payer,
# MAGIC   s.is_requester = b.is_requester,
# MAGIC   s.is_scheduler = b.is_scheduler,
# MAGIC   s.last_name = b.last_name,
# MAGIC   s.other_role = b.other_role,
# MAGIC   s.phone = b.phone,
# MAGIC   s.role = b.role,
# MAGIC   s.send_estimate = b.send_estimate,
# MAGIC   s.hashkey = b.hashkey
# MAGIC when not matched then insert (
# MAGIC   id,
# MAGIC   approval_type,
# MAGIC   approved_on,
# MAGIC   contact_id,
# MAGIC   email,
# MAGIC   first_name,
# MAGIC   is_initial_authorizer,
# MAGIC   is_payer,
# MAGIC   is_requester,
# MAGIC   is_scheduler,
# MAGIC   last_name,
# MAGIC   other_role,
# MAGIC   phone,
# MAGIC   role,
# MAGIC   send_estimate,
# MAGIC   hashkey
# MAGIC ) values (
# MAGIC   b.id,
# MAGIC   b.approval_type,
# MAGIC   from_unixtime(b.approved_on),
# MAGIC   b.contact_id,
# MAGIC   b.email,
# MAGIC   b.first_name,
# MAGIC   b.is_initial_authorizer,
# MAGIC   b.is_payer,
# MAGIC   b.is_requester,
# MAGIC   b.is_scheduler,
# MAGIC   b.last_name,
# MAGIC   b.other_role,
# MAGIC   b.phone,
# MAGIC   b.role,
# MAGIC   b.send_estimate,
# MAGIC   b.hashkey
# MAGIC )
# MAGIC when not matched by source then delete
