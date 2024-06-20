# Databricks notebook source
# MAGIC %sql
# MAGIC create table if not exists silver.company (
# MAGIC   id string,
# MAGIC   admin_name string,
# MAGIC   affiliate_id string,
# MAGIC   created_on timestamp,
# MAGIC   email string,
# MAGIC   has_custom_delivery_service boolean,
# MAGIC   institutional boolean,
# MAGIC   landing_page_url string,
# MAGIC   name string,
# MAGIC   phone long,
# MAGIC   use_domain_for_assignment boolean,
# MAGIC   logo_id string,
# MAGIC   partner_api_token string,
# MAGIC   hashkey string
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into silver.company s
# MAGIC using bronze.app_company b
# MAGIC on s.id = b.id
# MAGIC when matched and s.hashkey != b.hashkey then update set 
# MAGIC   s.id = b.id,
# MAGIC   s.admin_name = b.admin_name,
# MAGIC   s.affiliate_id = b.affiliate_id,
# MAGIC   s.created_on = from_unixtime(b.created_on),
# MAGIC   s.email = b.email,
# MAGIC   s.has_custom_delivery_service = b.has_custom_delivery_service,
# MAGIC   s.institutional = b.institutional,
# MAGIC   s.landing_page_url = b.landing_page_url,
# MAGIC   s.name = b.name,
# MAGIC   s.phone = b.phone,
# MAGIC   s.use_domain_for_assignment = b.use_domain_for_assignment,
# MAGIC   s.logo_id = b.logo_id,
# MAGIC   s.partner_api_token = b.partner_api_token,
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
