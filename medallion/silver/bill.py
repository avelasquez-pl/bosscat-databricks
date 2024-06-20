# Databricks notebook source
# MAGIC %sql
# MAGIC create table if not exists silver.bill (
# MAGIC   id string,
# MAGIC   amount double,
# MAGIC   company_name string,
# MAGIC   created_by string,
# MAGIC   created_on timestamp,
# MAGIC   description string,
# MAGIC   job_id string,
# MAGIC   order_id string,
# MAGIC   order_type string,
# MAGIC   payment_type string,
# MAGIC   pricing_item_id string,
# MAGIC   quick_books_account_id string,
# MAGIC   quick_books_department_id string,
# MAGIC   quick_books_id string,
# MAGIC   quick_books_job_customer_id string,
# MAGIC   quick_books_vendor_id string,
# MAGIC   vendor_email string,
# MAGIC   vendor_id string,
# MAGIC   hashkey string
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into silver.bill s
# MAGIC using bronze.app_bill b
# MAGIC on s.id = b.id
# MAGIC when matched and s.hashkey != b.hashkey then update set 
# MAGIC   s.id = b.id,
# MAGIC   s.amount = b.amount,
# MAGIC   s.company_name = b.company_name,
# MAGIC   s.created_by = b.created_by,
# MAGIC   s.created_on = from_unixtime(b.created_on),
# MAGIC   s.description = b.description,
# MAGIC   s.job_id = b.job_id,
# MAGIC   s.order_id = b.order_id,
# MAGIC   s.order_type = b.order_type,
# MAGIC   s.payment_type = b.payment_type,
# MAGIC   s.pricing_item_id = b.pricing_item_id,
# MAGIC   s.quick_books_account_id = b.quick_books_account_id,
# MAGIC   s.quick_books_department_id = b.quick_books_department_id,
# MAGIC   s.quick_books_id = b.quick_books_id,
# MAGIC   s.quick_books_job_customer_id = b.quick_books_job_customer_id,
# MAGIC   s.quick_books_vendor_id = b.quick_books_vendor_id,
# MAGIC   s.vendor_email = b.vendor_email,
# MAGIC   s.vendor_id = b.vendor_id,
# MAGIC   s.hashkey = b.hashkey
# MAGIC when not matched then insert (
# MAGIC   id,
# MAGIC   amount,
# MAGIC   company_name,
# MAGIC   created_by,
# MAGIC   created_on,
# MAGIC   description,
# MAGIC   job_id,
# MAGIC   order_id,
# MAGIC   order_type,
# MAGIC   payment_type,
# MAGIC   pricing_item_id,
# MAGIC   quick_books_account_id,
# MAGIC   quick_books_department_id,
# MAGIC   quick_books_id,
# MAGIC   quick_books_job_customer_id,
# MAGIC   quick_books_vendor_id,
# MAGIC   vendor_email,
# MAGIC   vendor_id,
# MAGIC   hashkey
# MAGIC ) values (
# MAGIC   b.id,
# MAGIC   b.amount,
# MAGIC   b.company_name,
# MAGIC   b.created_by,
# MAGIC   from_unixtime(b.created_on),
# MAGIC   b.description,
# MAGIC   b.job_id,
# MAGIC   b.order_id,
# MAGIC   b.order_type,
# MAGIC   b.payment_type,
# MAGIC   b.pricing_item_id,
# MAGIC   b.quick_books_account_id,
# MAGIC   b.quick_books_department_id,
# MAGIC   b.quick_books_id,
# MAGIC   b.quick_books_job_customer_id,
# MAGIC   b.quick_books_vendor_id,
# MAGIC   b.vendor_email,
# MAGIC   b.vendor_id,
# MAGIC   b.hashkey
# MAGIC )
# MAGIC when not matched by source then delete
