-- Generates sql to compare two tables using the audit_helper package
-- To run:
--      dbt compile --select compare_queries
--      then copy the output from CLI to DBeaver
--      Remove '' around sources
--      Execute SQL

{% set old_etl_relation=ref('customer_orders_legacy') %} 

{% set dbt_relation=ref('fct_customer_orders') %}  {{ 

audit_helper.compare_relations(
        a_relation=old_etl_relation,
        b_relation=dbt_relation,
        primary_key="order_id"
    ) }}