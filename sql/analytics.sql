-- Analytics Queries for the Data Mart (Gold Layer)
-- These queries demonstrate how to use the Star Schema to extract business value.

-- 1. Top 10 Revenue Categories
SELECT 
    p.product_category_name_english AS category, 
    COUNT(DISTINCT i.order_id) AS num_orders, 
    ROUND(SUM(pay.payment_value)::numeric, 2) AS total_revenue
FROM gold.fct_order_items i
JOIN gold.dim_products p ON i.product_id = p.product_id
JOIN gold.fct_order_payments pay ON i.order_id = pay.order_id
WHERE i.order_status = 'delivered'
  AND p.product_category_name_english IS NOT NULL
  AND i.order_delivered_customer_date IS NOT NULL
GROUP BY p.product_category_name_english
ORDER BY total_revenue DESC
LIMIT 10;

-- 2. Revenue by Month and Year
SELECT
    EXTRACT(MONTH FROM i.order_delivered_customer_date) AS month_no,
    TO_CHAR(i.order_delivered_customer_date, 'Mon') AS month_name,
    SUM(CASE WHEN EXTRACT(YEAR FROM i.order_delivered_customer_date) = 2016 THEN pay.payment_value ELSE 0 END) AS year_2016,
    SUM(CASE WHEN EXTRACT(YEAR FROM i.order_delivered_customer_date) = 2017 THEN pay.payment_value ELSE 0 END) AS year_2017,
    SUM(CASE WHEN EXTRACT(YEAR FROM i.order_delivered_customer_date) = 2018 THEN pay.payment_value ELSE 0 END) AS year_2018
FROM gold.fct_order_items i
JOIN (
    SELECT order_id, MIN(payment_value) as payment_value
    FROM gold.fct_order_payments
    GROUP BY order_id
) pay ON i.order_id = pay.order_id
WHERE i.order_delivered_customer_date IS NOT NULL
  AND i.order_status IN ('delivered', 'shipped', 'invoiced', 'approved')
GROUP BY 1, 2
ORDER BY 1 ASC;

-- 3. Delivery Time Discrepancy (Actual vs Estimated)
SELECT 
    AVG(EXTRACT(EPOCH FROM (order_delivered_customer_date - order_purchase_timestamp))/86400) AS avg_actual_delivery_days,
    AVG(EXTRACT(EPOCH FROM (order_estimated_delivery_date - order_purchase_timestamp))/86400) AS avg_estimated_delivery_days
FROM gold.fct_order_items
WHERE order_status = 'delivered' 
  AND order_delivered_customer_date IS NOT NULL;

-- 4. Top 5 Products by Revenue
SELECT 
    p.product_id,
    p.product_category_name_english,
    SUM(i.price) AS total_revenue
FROM gold.fct_order_items i
JOIN gold.dim_products p ON i.product_id = p.product_id
WHERE i.order_status = 'delivered'
GROUP BY p.product_id, p.product_category_name_english
ORDER BY total_revenue DESC
LIMIT 5;

-- 5. Top Spending Customers
SELECT 
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    SUM(pay.payment_value) AS total_spent
FROM gold.fct_order_payments pay
JOIN gold.dim_customers c ON pay.customer_id = c.customer_id
GROUP BY c.customer_unique_id, c.customer_city, c.customer_state
ORDER BY total_spent DESC
LIMIT 10;
