-- *************************************************************************************************************************************
-- ===============================================*** Advanced Data Analytics (START) ***===============================================
-- *************************************************************************************************************************************

-- ========================== Changes Over Time (START) =============================

-- Analyze Sales performance Over Time
SELECT 
YEAR(order_date) AS order_year,
MONTH(order_date) AS order_month,
sum(sales_amount) AS total_sales,
COUNT(DISTINCT customer_key) AS total_customers,
sum(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY order_year,order_month
ORDER BY order_year,order_month;

-- =========================== Changes Over Time (END) ==============================


-- ========================= Cumulative Analysis (START) ============================

-- Calculate the total sales per month 
-- and the running total of sales over time
SELECT 
order_month,
total_sales,
sum(total_sales) OVER(ORDER BY order_month) AS running_total_sales,
ROUND(AVG(avg_price) OVER(ORDER BY order_month)) AS moving_average_price
FROM
(
SELECT 
DATE_FORMAT(order_date, '%Y-%m-01') AS order_month,
SUM(sales_amount) AS total_sales,
AVG(price) AS avg_price
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY order_month
ORDER BY order_month
) t;

-- ========================== Cumulative Analysis (END) =============================


-- ========================= Performance Analysis (START) ===========================

/* 
Analyze the yearly performance of products by comparing their sales 
to both the average sales performance of the product and the previous year's sales.
*/
WITH yearly_product_sales AS( 
SELECT
YEAR(f.order_date) AS order_year,
p.product_name,
SUm(f.sales_amount) AS current_sales
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_products AS p
ON f.product_key = p.product_key
WHERE f.order_date IS NOT NULL
GROUP BY order_year,p.product_name
)
SELECT 
order_year,
product_name,
current_sales,
ROUND(avg(current_sales) over(PARTITION BY product_name )) AS avg_sales,
current_sales - ROUND(avg(current_sales) over(PARTITION BY product_name )) AS diff_avg,
CASE WHEN current_sales - ROUND(avg(current_sales) over(PARTITION BY product_name )) > 0 THEN 'Above Avg'
	 WHEN current_sales - ROUND(avg(current_sales) over(PARTITION BY product_name )) < 0 THEN 'Below Avg'
     ELSE 'Avg'
END AS avg_change,
     -- Year-over-year Analysis
LAG(current_sales) over(PARTITION BY product_name ORDER BY order_year ) AS py_sales,
current_sales - LAG(current_sales) over(PARTITION BY product_name ORDER BY order_year ) AS diff_py,
CASE WHEN current_sales - LAG(current_sales) over(PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
	 WHEN current_sales - LAG(current_sales) over(PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
     ELSE 'No change'
END AS py_hange
FROM yearly_product_sales
ORDER BY product_name,order_year;

-- ========================= Performance Analysis (END) =============================


-- ================= Part-To-Whole (Proporional Analysis) (START) ===================

-- Which categories ontribute the most to overall sales ?
WITH category_sales AS(
SELECT 
category,
SUM(sales_amount) AS total_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products AS p
ON p.product_key = f.product_key
GROUP BY category
)
SELECT 
category,
total_sales,
SUM(total_sales) OVER() AS overall_sales,
CONCAT(ROUND((total_sales/SUM(total_sales) OVER())*100,2),'%')AS perentage_of_total
FROM category_sales
ORDER BY total_sales DESC;

-- ================== Part-To-Whole (Proporional Analysis) (END) ====================



-- ========================== Data Segmentation (START) =============================
/*
Segment products into cost ranges and
count how many products fall into each segment
*/
WITH product_segments AS (
    SELECT
        product_key,
        product_name,
        cost,
        CASE 
            WHEN cost < 100 THEN 'Below 100'
            WHEN cost BETWEEN 100 AND 500 THEN '100-500'
            WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
            ELSE 'Above 1000'
        END AS cost_range
    FROM gold.dim_products
)
SELECT
cost_range,
COUNT(product_key) AS total_products
FROM product_segments
GROUP BY cost_range
ORDER BY total_products DESC;
/*
Group customers into three segments based on their spending behavior:

- VIP: Customers with at least 12 months of history and spending more than €5,000.
- Regular: Customers with at least 12 months of history but spending €5,000 or less.
- New: Customers with a lifespan less than 12 months.

Find the total number of customers in each group.
*/

WITH customer_spending AS (
SELECT 
c.customer_key,
SUM(f.sales_amount) AS total_spending,
MIN(f.order_date) AS first_order,
MAX(f.order_date) AS last_order,
TIMESTAMPDIFF(MONTH,MIN(f.order_date),MAX(f.order_date)) AS lifespan
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
ON f.customer_key = c.customer_key
GROUP BY c.customer_key
)
SELECT 
customer_key,
total_spending,
lifespan,
CASE WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
	 WHEN lifespan <= 12 AND total_spending <= 5000 THEN 'Regular'
     ELSE 'New'
END AS customer_segment
FROM customer_spending;

-- Total Customers in each Segment
WITH customer_spending AS (
SELECT 
c.customer_key,
SUM(f.sales_amount) AS total_spending,
MIN(f.order_date) AS first_order,
MAX(f.order_date) AS last_order,
TIMESTAMPDIFF(MONTH,MIN(f.order_date),MAX(f.order_date)) AS lifespan
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
ON f.customer_key = c.customer_key
GROUP BY c.customer_key
)
SELECT 
customer_segment,
COUNT(customer_key) AS total_customers
FROM (
SELECT 
customer_key,
CASE WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
	 WHEN lifespan <= 12 AND total_spending <= 5000 THEN 'Regular'
     ELSE 'New'
END AS customer_segment
FROM customer_spending) t 
GROUP BY customer_segment
ORDER BY total_customers DESC;
-- ============================ Data Segmentation (END) =============================


-- *************************************************************************************************************************************
-- ================================================*** Advanced Data Analytics (END) ***================================================
-- *************************************************************************************************************************************

