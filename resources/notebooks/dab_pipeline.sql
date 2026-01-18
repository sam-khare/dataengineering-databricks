-- Databricks notebook source
-- Create streaming table
CREATE OR REFRESH STREAMING TABLE st_orders
AS
SELECT * FROM STREAM(samples.tpch.orders)

-- COMMAND ----------

-- create MV
CREATE OR REPLACE MATERIALIZED VIEW agg_orders
AS
SELECT 
COUNT(o_orderkey) as cnt_orders,
o_orderstatus
from LIVE.st_orders
GROUP BY o_orderstatus
