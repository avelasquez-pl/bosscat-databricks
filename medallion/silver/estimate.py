# Databricks notebook source
# MAGIC %sql
# MAGIC create table if not exists silver.estimate (
# MAGIC   id string,
# MAGIC   approval_status string,
# MAGIC   approved_by string,
# MAGIC   approved_on timestamp,
# MAGIC   awaiting_follow_up boolean,
# MAGIC   created_by string,
# MAGIC   created_on timestamp,
# MAGIC   currently_in_use boolean,
# MAGIC   delivered_on timestamp,
# MAGIC   delivery_service_cost double,
# MAGIC   due_on timestamp,
# MAGIC   estimator_id string,
# MAGIC   expedited boolean,
# MAGIC   hold_notes string,
# MAGIC   is_evaluation boolean,
# MAGIC   job_needs_attention boolean,
# MAGIC   last_modified_by string,
# MAGIC   last_modified_on timestamp,
# MAGIC   last_note string,
# MAGIC   on_hold boolean,
# MAGIC   promotion_id string,
# MAGIC   public_status string,
# MAGIC   repair_timeline string,
# MAGIC   search_index string,
# MAGIC   status string,
# MAGIC   delivery_service_id string,
# MAGIC   partner_api_request_id string,
# MAGIC   partner_api_token string,
# MAGIC   last_delivered_on timestamp,
# MAGIC   calendly_event_id string,
# MAGIC   hashkey string
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into silver.estimate s
# MAGIC using bronze.app_estimate b
# MAGIC on s.id = b.id
# MAGIC when matched and s.hashkey != b.hashkey then update set 
# MAGIC   s.id = b.id,
# MAGIC   s.approval_status = b.approval_status,
# MAGIC   s.approved_by = b.approved_by,
# MAGIC   s.approved_on = from_unixtime(b.approved_on),
# MAGIC   s.awaiting_follow_up = b.awaiting_follow_up,
# MAGIC   s.created_by = b.created_by,
# MAGIC   s.created_on = from_unixtime(b.created_on),
# MAGIC   s.currently_in_use = b.currently_in_use,
# MAGIC   s.delivered_on = from_unixtime(b.delivered_on),
# MAGIC   s.delivery_service_cost = b.delivery_service_cost,
# MAGIC   s.due_on = from_unixtime(b.due_on),
# MAGIC   s.estimator_id = b.estimator_id,
# MAGIC   s.expedited = b.expedited,
# MAGIC   s.hold_notes = b.hold_notes,
# MAGIC   s.is_evaluation = b.is_evaluation,
# MAGIC   s.job_needs_attention = b.job_needs_attention,
# MAGIC   s.last_modified_by = b.last_modified_by,
# MAGIC   s.last_modified_on = b.last_modified_on,
# MAGIC   s.last_note = b.last_note,
# MAGIC   s.on_hold = b.on_hold,
# MAGIC   s.promotion_id = b.promotion_id,
# MAGIC   s.public_status = b.public_status,
# MAGIC   s.repair_timeline = b.repair_timeline,
# MAGIC   s.search_index = b.search_index,
# MAGIC   s.status = b.status,
# MAGIC   s.delivery_service_id = b.delivery_service_id,
# MAGIC   s.partner_api_request_id = b.partner_api_request_id,
# MAGIC   s.partner_api_token = b.partner_api_token,
# MAGIC   s.last_delivered_on = from_unixtime(b.last_delivered_on),
# MAGIC   s.calendly_event_id = b.calendly_event_id,
# MAGIC   s.hashkey = b.hashkey
# MAGIC when not matched then insert (
# MAGIC   id,
# MAGIC   approval_status,
# MAGIC   approved_by,
# MAGIC   approved_on,
# MAGIC   awaiting_follow_up,
# MAGIC   created_by,
# MAGIC   created_on,
# MAGIC   currently_in_use,
# MAGIC   delivered_on,
# MAGIC   delivery_service_cost,
# MAGIC   due_on,
# MAGIC   estimator_id,
# MAGIC   expedited,
# MAGIC   hold_notes,
# MAGIC   is_evaluation,
# MAGIC   job_needs_attention,
# MAGIC   last_modified_by,
# MAGIC   last_modified_on,
# MAGIC   last_note,
# MAGIC   on_hold,
# MAGIC   promotion_id,
# MAGIC   public_status,
# MAGIC   repair_timeline,
# MAGIC   search_index,
# MAGIC   status,
# MAGIC   delivery_service_id,
# MAGIC   partner_api_request_id,
# MAGIC   partner_api_token,
# MAGIC   last_delivered_on,
# MAGIC   calendly_event_id,
# MAGIC   hashkey
# MAGIC ) values (
# MAGIC   b.id,
# MAGIC   b.approval_status,
# MAGIC   b.approved_by,
# MAGIC   from_unixtime(b.approved_on),
# MAGIC   b.awaiting_follow_up,
# MAGIC   b.created_by,
# MAGIC   from_unixtime(b.created_on),
# MAGIC   b.currently_in_use,
# MAGIC   from_unixtime(b.delivered_on),
# MAGIC   b.delivery_service_cost,
# MAGIC   from_unixtime(b.due_on),
# MAGIC   b.estimator_id,
# MAGIC   b.expedited,
# MAGIC   b.hold_notes,
# MAGIC   b.is_evaluation,
# MAGIC   b.job_needs_attention,
# MAGIC   b.last_modified_by,
# MAGIC   b.last_modified_on,
# MAGIC   b.last_note,
# MAGIC   b.on_hold,
# MAGIC   b.promotion_id,
# MAGIC   b.public_status,
# MAGIC   b.repair_timeline,
# MAGIC   b.search_index,
# MAGIC   b.status,
# MAGIC   b.delivery_service_id,
# MAGIC   b.partner_api_request_id,
# MAGIC   b.partner_api_token,
# MAGIC   from_unixtime(b.last_delivered_on),
# MAGIC   b.calendly_event_id,
# MAGIC   b.hashkey
# MAGIC )
# MAGIC when not matched by source then delete
