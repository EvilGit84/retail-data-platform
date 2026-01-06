-- 1. Total Revenue
CREATE OR REPLACE VIEW v_total_revenue AS
SELECT
    SUM(amount) AS total_revenue
FROM retail_orders;


-- 2. City-wise Revenue
CREATE OR REPLACE VIEW v_city_revenue AS
SELECT
    city,
    COUNT(*) AS total_orders,
    SUM(amount) AS total_revenue
FROM retail_orders
GROUP BY city
ORDER BY total_revenue DESC;


-- 3. Daily Orders Trend
CREATE OR REPLACE VIEW v_daily_orders AS
SELECT
    DATE(created_at) AS order_date,
    COUNT(*) AS total_orders,
    SUM(amount) AS daily_revenue
FROM retail_orders
GROUP BY DATE(created_at)
ORDER BY order_date;


-- 4. City-wise Daily Sales
CREATE OR REPLACE VIEW v_city_daily_sales AS
SELECT
    city,
    DATE(created_at) AS order_date,
    COUNT(*) AS total_orders,
    SUM(amount) AS revenue
FROM retail_orders
GROUP BY city, DATE(created_at)
ORDER BY order_date, city;
