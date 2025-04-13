-- E-commerce Data Analysis Queries
-- This file contains various SQL queries to analyze the e-commerce dataset

-- 1. CUSTOMER ANALYSIS

-- 1.1 Customer segments distribution
SELECT 
    segment,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM customers), 2) AS percentage
FROM customers
GROUP BY segment
ORDER BY customer_count DESC;

-- 1.2 Top 10 customers by lifetime value
SELECT 
    customer_id,
    name,
    email,
    lifetime_value
FROM customers
ORDER BY lifetime_value DESC
LIMIT 10;

-- 1.3 Customer acquisition over time (by registration date)
SELECT 
    strftime('%Y-%m', registration_date) AS month,
    COUNT(*) AS new_customers
FROM customers
GROUP BY month
ORDER BY month;

-- 2. PRODUCT ANALYSIS

-- 2.1 Product category distribution
SELECT 
    category,
    COUNT(*) AS product_count,
    ROUND(AVG(price), 2) AS avg_price,
    MIN(price) AS min_price,
    MAX(price) AS max_price
FROM products
GROUP BY category
ORDER BY product_count DESC;

-- 2.2 Top selling products by quantity sold
SELECT 
    p.product_id,
    p.name,
    p.category,
    SUM(oi.quantity) AS units_sold,
    COUNT(DISTINCT oi.order_id) AS order_count,
    SUM(oi.total) AS revenue
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
GROUP BY p.product_id
ORDER BY units_sold DESC
LIMIT 20;

-- 2.3 Products with no sales
SELECT 
    p.product_id,
    p.name,
    p.category,
    p.price
FROM products p
LEFT JOIN order_items oi ON p.product_id = oi.product_id
WHERE oi.order_id IS NULL;

-- 3. ORDER ANALYSIS

-- 3.1 Order counts and revenue by status
SELECT 
    status,
    COUNT(*) AS order_count,
    ROUND(SUM(total), 2) AS total_revenue,
    ROUND(AVG(total), 2) AS avg_order_value
FROM orders
GROUP BY status
ORDER BY order_count DESC;

-- 3.2 Monthly sales trends
SELECT 
    strftime('%Y-%m', order_date) AS month,
    COUNT(*) AS order_count,
    ROUND(SUM(total), 2) AS monthly_revenue,
    ROUND(AVG(total), 2) AS avg_order_value
FROM orders
GROUP BY month
ORDER BY month;

-- 3.3 Orders by day of week (0 = Sunday, 6 = Saturday)
SELECT 
    strftime('%w', order_date) AS day_of_week,
    COUNT(*) AS order_count,
    ROUND(SUM(total), 2) AS total_revenue
FROM orders
GROUP BY day_of_week
ORDER BY day_of_week;

-- 3.4 Orders with multiple items
SELECT 
    o.order_id,
    o.customer_id,
    c.name AS customer_name,
    o.order_date,
    COUNT(oi.product_id) AS item_count,
    o.total
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY o.order_id
HAVING item_count > 1
ORDER BY item_count DESC
LIMIT 20;

-- 4. COMPLEX ANALYSIS

-- 4.1 Category performance by customer segment
SELECT 
    c.segment,
    p.category,
    COUNT(DISTINCT o.order_id) AS order_count,
    COUNT(DISTINCT c.customer_id) AS customer_count,
    ROUND(SUM(oi.total), 2) AS total_revenue,
    ROUND(AVG(oi.total), 2) AS avg_item_value
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
GROUP BY c.segment, p.category
ORDER BY c.segment, total_revenue DESC;

-- 4.2 Repeat purchase analysis
SELECT 
    purchase_count,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM customers), 2) AS percentage
FROM (
    SELECT 
        c.customer_id,
        COUNT(DISTINCT o.order_id) AS purchase_count
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id
) AS customer_orders
GROUP BY purchase_count
ORDER BY purchase_count;

-- 4.3 Average time between orders for repeat customers
WITH customer_orders AS (
    SELECT 
        customer_id,
        order_id,
        order_date,
        LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_order_date
    FROM orders
)
SELECT 
    AVG(JULIANDAY(order_date) - JULIANDAY(prev_order_date)) AS avg_days_between_orders
FROM customer_orders
WHERE prev_order_date IS NOT NULL;

-- 4.4 Revenue contribution by customer segment and time
SELECT 
    strftime('%Y-%m', o.order_date) AS month,
    c.segment,
    COUNT(DISTINCT o.order_id) AS order_count,
    ROUND(SUM(o.total), 2) AS total_revenue,
    ROUND(SUM(o.total) * 100.0 / SUM(SUM(o.total)) OVER (PARTITION BY strftime('%Y-%m', o.order_date)), 2) AS revenue_percentage
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY month, c.segment
ORDER BY month, total_revenue DESC;