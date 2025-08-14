-- Ecommerce database schema with modular imports
CREATE DATABASE IF NOT EXISTS ecommerce ON CLUSTER demo ENGINE = Atomic COMMENT 'E-commerce analytics database';

-- Import table definitions
-- housekeeper:import tables/categories_source.sql
-- housekeeper:import tables/user_segments_source.sql
-- housekeeper:import tables/users.sql
-- housekeeper:import tables/events.sql
-- housekeeper:import tables/products.sql
-- housekeeper:import tables/orders.sql
-- housekeeper:import tables/order_items.sql

-- Import dictionary definitions
-- housekeeper:import dictionaries/countries.sql
-- housekeeper:import dictionaries/categories.sql
-- housekeeper:import dictionaries/user_segments.sql

-- Import view definitions  
-- #housekeeper:import views/daily_sales.sql
-- #housekeeper:import views/user_activity.sql
-- #housekeeper:import views/top_products.sql
-- #housekeeper:import views/mv_product_stats.sql
-- #housekeeper:import views/mv_hourly_events.sql
