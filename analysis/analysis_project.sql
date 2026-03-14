-- =====================================================================
-- INITIAL DATABASE & SCHEMA EXPLORATION
-- =====================================================================
-- Purpose: Understand what tables and columns exist in the data warehouse
-- Very useful at the beginning of any new analytics project

-- Explore all Objects in the database
SELECT * FROM INFORMATION_SCHEMA.TABLES

-- Explore all Columns in the database (focused on dim_customers)
SELECT * FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_customers'


-- =====================================================================
-- DIMENSION EXPLORATION
-- =====================================================================
-- Goal: Understand cardinality and possible grouping/segmentation keys
-- These queries help identify dimensions we can slice & dice by

-- Explore all countries our customers come from
SELECT DISTINCT country FROM gold.dim_customers

-- Explore all Categories & Subcategories ('The major Divisions')
SELECT DISTINCT category, subcategory
FROM gold.dim_products


-- =====================================================================
-- DATE / TIME RANGE EXPLORATION
-- =====================================================================
-- Goal: Know the temporal coverage of the dataset

-- Find the date of the first and last order + overall range in years
SELECT
    MIN(order_date)           AS first_order_date,
    MAX(order_date)           AS last_order_date,
    DATEDIFF(year, MIN(order_date), MAX(order_date)) AS order_range_years
FROM gold.fact_sales


-- =====================================================================
-- CUSTOMER AGE / DEMOGRAPHIC EXPLORATION
-- =====================================================================

-- Find the youngest and the oldest customer (based on birthdate)
SELECT
    MIN(birthdate)                              AS oldest_customer,
    DATEDIFF(year, MIN(birthdate), GETDATE())   AS oldest_age,
    MAX(birthdate)                              AS youngest_customer,
    DATEDIFF(year, MAX(birthdate), GETDATE())   AS youngest_age
FROM gold.dim_customers


-- =====================================================================
-- BASIC FACT TABLE MEASURES (KPI Exploration)
-- =====================================================================
-- Goal: Calculate the most important headline business metrics

-- Find the Total Sales (revenue)
SELECT SUM(sales_amount) AS total_sales
FROM gold.fact_sales

-- Find how many items are sold (total units)
SELECT SUM(quantity) AS items_sold
FROM gold.fact_sales

-- Find the average selling price
SELECT AVG(price) AS avg_price
FROM gold.fact_sales

-- Find the Total number of Orders
SELECT COUNT(DISTINCT order_number) AS total_orders
FROM gold.fact_sales

-- Dimension cardinalities (reference numbers)
SELECT COUNT(product_key)   AS total_products   FROM gold.dim_products
SELECT COUNT(customer_key)  AS total_customers  FROM gold.dim_customers

-- Active / transacting customers
SELECT COUNT(DISTINCT customer_key) AS customers_placed_orders
FROM gold.fact_sales


-- =====================================================================
-- SINGLE-QUERY BUSINESS HEALTH DASHBOARD
-- =====================================================================
-- Goal: One query that returns all key headline metrics (good for quick view / monitoring)

SELECT 'Total Sales'              AS measure_name, SUM(sales_amount)               AS measure_value FROM gold.fact_sales UNION ALL
SELECT 'Total Quantity'           AS measure_name, SUM(quantity)                  AS measure_value FROM gold.fact_sales UNION ALL
SELECT 'Average Price'            AS measure_name, AVG(price)                     AS measure_value FROM gold.fact_sales UNION ALL
SELECT 'Total Orders'             AS measure_name, COUNT(DISTINCT order_number)   AS measure_value FROM gold.fact_sales UNION ALL
SELECT 'Total products'           AS measure_name, COUNT(product_key)             AS measure_value FROM gold.dim_products UNION ALL
SELECT 'Total Customers'          AS measure_name, COUNT(customer_key)            AS measure_value FROM gold.dim_customers UNION ALL
SELECT 'Customers Placed Orders'  AS measure_name, COUNT(DISTINCT customer_key)   AS measure_value FROM gold.fact_sales


-- =====================================================================
-- MAGNITUDE & SEGMENTATION ANALYSIS
-- =====================================================================
-- Goal: Understand distribution of customers, products, revenue across key dimensions

-- Total customers by country
SELECT country, COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY country

-- Total customers by gender
SELECT gender, COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY gender

-- Total products by category
SELECT category, COUNT(product_key) AS total_products
FROM gold.dim_products
GROUP BY category

-- Average cost per category
SELECT category, AVG(cost) AS avg_cost
FROM gold.dim_products
GROUP BY category

-- Total revenue by category
SELECT
    p.category,
    SUM(s.sales_amount) AS total_revenue
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p ON p.product_key = s.product_key
GROUP BY p.category

-- Total revenue per customer (top customers identification)
SELECT
    c.customer_key,
    c.first_name,
    c.last_name,
    SUM(s.sales_amount) AS total_revenue
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c ON c.customer_key = s.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name

-- Quantity sold (items) distribution across countries
SELECT
    c.country,
    SUM(s.quantity) AS total_items_sold        -- ← fixed misleading column alias
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c ON c.customer_key = s.customer_key
GROUP BY c.country

-- Top 5 highest revenue products (two equivalent styles)
SELECT TOP 5
    p.product_name,
    SUM(s.sales_amount) AS total_revenue
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p ON p.product_key = s.product_key
GROUP BY p.product_name
ORDER BY total_revenue DESC

-- Alternative using window function (more flexible for pagination)
SELECT *
FROM (
    SELECT
        p.product_name,
        SUM(s.sales_amount) AS total_revenue,
        ROW_NUMBER() OVER (ORDER BY SUM(s.sales_amount) DESC) AS rank_products
    FROM gold.fact_sales s
    LEFT JOIN gold.dim_products p ON p.product_key = s.product_key
    GROUP BY p.product_name
) t
WHERE rank_products <= 5

-- Bottom 5 worst performing products
SELECT TOP 5
    p.product_name,
    SUM(s.sales_amount) AS total_revenue
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p ON p.product_key = s.product_key
GROUP BY p.product_name
ORDER BY total_revenue ASC


-- Top 10 revenue customers + bottom 3 customers by order count
SELECT TOP 10
    c.customer_key, c.first_name, c.last_name,
    SUM(s.sales_amount) AS total_revenue
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c ON c.customer_key = s.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name
ORDER BY total_revenue DESC

SELECT TOP 3
    c.customer_key, c.first_name, c.last_name,
    COUNT(DISTINCT order_number) AS total_orders
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c ON c.customer_key = s.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name
ORDER BY total_orders ASC


-- =====================================================================
-- TIME-BASED & TREND ANALYSIS
-- =====================================================================

-- Sales & customer count by year
SELECT
    YEAR(order_date) AS order_year,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS unique_customers
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date)
ORDER BY order_year


-- Monthly sales with running total (cumulative sales)
SELECT
    order_month,
    total_sales,
    SUM(total_sales) OVER (ORDER BY order_month) AS cumulative_sales
FROM (
    SELECT
        DATETRUNC(MONTH, order_date) AS order_month,
        SUM(sales_amount) AS total_sales
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY DATETRUNC(MONTH, order_date)
) t


-- =====================================================================
-- ADVANCED ANALYTICS – PRODUCT PERFORMANCE COMPARISON
-- =====================================================================

-- Yearly product performance vs average & previous year
WITH yearly_product_sales AS (
    SELECT
        YEAR(s.order_date) AS order_year,
        p.product_name,
        SUM(s.sales_amount) AS current_sales
    FROM gold.fact_sales s
    LEFT JOIN gold.dim_products p ON s.product_key = p.product_key
    WHERE s.order_date IS NOT NULL
    GROUP BY YEAR(s.order_date), p.product_name
)
SELECT
    order_year,
    product_name,
    current_sales,
    AVG(current_sales) OVER (PARTITION BY product_name) AS product_lifetime_avg,
    current_sales - AVG(current_sales) OVER (PARTITION BY product_name) AS diff_from_avg,
    CASE
        WHEN current_sales > AVG(current_sales) OVER (PARTITION BY product_name) THEN 'Above Avg'
        WHEN current_sales < AVG(current_sales) OVER (PARTITION BY product_name) THEN 'Below Avg'
        ELSE 'Equal to Avg'
    END AS vs_lifetime_avg,
    LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS prev_year_sales,
    current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS yoy_change,
    CASE
        WHEN current_sales > LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) THEN '↑ Increase'
        WHEN current_sales < LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) THEN '↓ Decrease'
        ELSE '— Flat'
    END AS yoy_trend
FROM yearly_product_sales
ORDER BY product_name, order_year


-- =====================================================================
-- PART-TO-WHOLE ANALYSIS (Category contribution)
-- =====================================================================

WITH category_sales AS (
    SELECT
        p.category,
        SUM(s.sales_amount) AS category_sales
    FROM gold.fact_sales s
    LEFT JOIN gold.dim_products p ON s.product_key = p.product_key
    GROUP BY p.category
)
SELECT
    category,
    category_sales,
    SUM(category_sales) OVER () AS total_company_sales,
    ROUND(100.0 * category_sales / SUM(category_sales) OVER (), 2) AS pct_of_total
FROM category_sales
ORDER BY category_sales DESC


-- =====================================================================
-- SEGMENTATION EXAMPLES
-- =====================================================================

-- Product cost segmentation
WITH product_segment AS (
    SELECT
        product_key,
        product_name,
        cost,
        CASE
            WHEN cost < 100          THEN 'Below 100'
            WHEN cost <= 500         THEN '100–500'
            WHEN cost <= 1000        THEN '501–1000'
            ELSE 'Above 1000'
        END AS cost_range
    FROM gold.dim_products
)
SELECT
    cost_range,
    COUNT(*) AS product_count
FROM product_segment
GROUP BY cost_range
ORDER BY MIN(cost)


-- Customer segmentation (VIP / Regular / New)
WITH customer_spending AS (
    SELECT
        c.customer_key,
        SUM(s.sales_amount) AS total_spending,
        MIN(s.order_date)   AS first_order,
        MAX(s.order_date)   AS last_order,
        DATEDIFF(MONTH, MIN(s.order_date), MAX(s.order_date)) AS lifespan_months
    FROM gold.fact_sales s
    LEFT JOIN gold.dim_customers c ON c.customer_key = s.customer_key
    GROUP BY c.customer_key
)
SELECT
    customer_segment,
    COUNT(*) AS customer_count
FROM (
    SELECT
        customer_key,
        CASE
            WHEN lifespan_months > 12 AND total_spending > 5000  THEN 'VIP'
            WHEN lifespan_months >= 12                           THEN 'Regular'
            ELSE 'New'
        END AS customer_segment
    FROM customer_spending
) t
GROUP BY customer_segment


-- =====================================================================
-- FINAL REPORTING VIEWS (Reusable / Dashboard-ready)
-- =====================================================================

-- Customer-level reporting view with segments & KPIs
CREATE VIEW gold.report_customers AS
WITH base AS (
    SELECT
        s.order_number, s.product_key, s.order_date, s.sales_amount, s.quantity,
        c.customer_key, c.customer_number,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        DATEDIFF(YEAR, c.birthdate, GETDATE()) AS age
    FROM gold.fact_sales s
    LEFT JOIN gold.dim_customers c ON s.customer_key = c.customer_key
    WHERE s.order_date IS NOT NULL
),
customer_metrics AS (
    SELECT
        customer_key, customer_number, customer_name, age,
        COUNT(DISTINCT order_number)    AS total_orders,
        SUM(sales_amount)               AS total_sales,
        SUM(quantity)                   AS total_quantity,
        COUNT(DISTINCT product_key)     AS distinct_products,
        MAX(order_date)                 AS last_order_date,
        DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan_months
    FROM base
    GROUP BY customer_key, customer_number, customer_name, age
)
SELECT
    *,
    CASE
        WHEN age < 20          THEN 'Under 20'
        WHEN age <= 29         THEN '20–29'
        WHEN age <= 39         THEN '30–39'
        WHEN age <= 49         THEN '40–49'
        ELSE '50+'
    END AS age_group,
    CASE
        WHEN lifespan_months > 12 AND total_sales > 5000 THEN 'VIP'
        WHEN lifespan_months >= 12 THEN 'Regular'
        ELSE 'New'
    END AS customer_segment,
    DATEDIFF(MONTH, last_order_date, GETDATE()) AS recency_months,
    CASE WHEN total_orders = 0 THEN 0 ELSE total_sales / total_orders END AS avg_order_value,
    CASE WHEN lifespan_months = 0 THEN total_sales ELSE total_sales * 1.0 / lifespan_months END AS avg_monthly_spend
FROM customer_metrics;


-- Product-level reporting view with segments & KPIs
CREATE VIEW gold.report_products AS
WITH base AS (
    SELECT
        s.order_number, s.customer_key, s.order_date, s.sales_amount, s.quantity,
        p.product_key, p.product_number, p.product_name, p.category, p.subcategory, p.cost
    FROM gold.fact_sales s
    LEFT JOIN gold.dim_products p ON p.product_key = s.product_key
    WHERE s.order_date IS NOT NULL
),
product_metrics AS (
    SELECT
        product_key, product_number, product_name, category, subcategory,
        COUNT(DISTINCT order_number)    AS total_orders,
        SUM(sales_amount)               AS total_sales,
        SUM(quantity)                   AS total_quantity_sold,
        COUNT(DISTINCT customer_key)    AS unique_customers,
        MAX(order_date)                 AS last_sale_date,
        DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan_months,
        ROUND(AVG(sales_amount * 1.0 / NULLIF(quantity, 0)), 2) AS avg_unit_price
    FROM base
    GROUP BY product_key, product_number, product_name, category, subcategory
)
SELECT
    *,
    CASE
        WHEN total_sales > 5000   THEN 'High Performer'
        WHEN total_sales >= 2000  THEN 'Mid-Range'
        ELSE 'Low Performer'
    END AS performance_segment,
    DATEDIFF(MONTH, last_sale_date, GETDATE()) AS recency_months,
    CASE WHEN total_orders = 0 THEN 0 ELSE total_sales / total_orders END AS avg_order_revenue,
    CASE WHEN lifespan_months = 0 THEN total_sales ELSE total_sales * 1.0 / lifespan_months END AS avg_monthly_revenue
FROM product_metrics;


-- Quick check
-- SELECT TOP 100 * FROM gold.report_customers ORDER BY total_sales DESC;
-- SELECT TOP 100 * FROM gold.report_products  ORDER BY total_sales DESC;
