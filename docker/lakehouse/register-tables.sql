-- Register the Delta directories initialized by the path-based pipeline.
-- Explicit LOCATION attaches existing data; managed CREATE TABLE rejects a
-- pre-populated default directory. These paths are still inside the warehouse.
CREATE DATABASE IF NOT EXISTS ecommerce;
CREATE TABLE IF NOT EXISTS ecommerce.bronze_events
USING DELTA LOCATION '/lakehouse/warehouse/ecommerce.db/bronze_events';
CREATE TABLE IF NOT EXISTS ecommerce.payment_events
USING DELTA LOCATION '/lakehouse/warehouse/ecommerce.db/payment_events';
CREATE TABLE IF NOT EXISTS ecommerce.daily_payments
USING DELTA LOCATION '/lakehouse/warehouse/ecommerce.db/daily_payments';
