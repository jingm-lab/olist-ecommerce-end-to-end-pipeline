create database ecommerce

create schema ecommerce.raw

create table customers (v variant);
create table geolocations (v variant);
create table order_items (v variant);
create table order_payments (v variant);
create table order_reviews (v variant);
create table orders (v variant);
create table product_category_name_translation (v variant);
create table products (v variant);
create table sellers (v variant);

create storage integration ECOMMERCE_GCS_INT
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'GCS'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('gcs://olist-ecommerce-bucket-datasets/')
    COMMENT = 'Integration for GCS';

desc storage integration ECOMMERCE_GCS_INT;

create notification integration ECOMMERCE_GCS_NOTIFY_INT
    ENABLED = TRUE
    TYPE = QUEUE
    NOTIFICATION_PROVIDER = GCP_PUBSUB
    GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/ecommerce-pipeline-488018/subscriptions/snowflake_notification-sub'
    COMMENT = 'Notification integration for gcs auto-ingest';

DESC NOTIFICATION INTEGRATION ECOMMERCE_GCS_NOTIFY_INT;

CREATE STAGE ECOMMERCE_GCS_STAGE
    URL = 'gcs://olist-ecommerce-bucket-datasets/'
    STORAGE_INTEGRATION = ECOMMERCE_GCS_INT
    FILE_FORMAT = (TYPE = PARQUET)

-- Change PIPE_EXECUTION_PAUSED to TRUE to stop snowpipe from running and save credits
ALTER PIPE ECOMMERCE.RAW.CUSTOMERS_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER PIPE ECOMMERCE.RAW.GEOLOCATIONS_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER PIPE ECOMMERCE.RAW.ORDERS_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER PIPE ECOMMERCE.RAW.ORDER_ITEMS_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER PIPE ECOMMERCE.RAW.ORDER_PAYMENTS_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER PIPE ECOMMERCE.RAW.ORDER_REVIEWS_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER PIPE ECOMMERCE.RAW.PRODUCTS_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER PIPE ECOMMERCE.RAW.PRODUCT_CATEGORY_NAME_TRANSLATION_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER PIPE ECOMMERCE.RAW.SELLERS_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;

LIST @ECOMMERCE_GCS_STAGE
