-- *************************************************************************************************************************************
-- ======================================================*** GOLD LAYER (START) ***=====================================================
-- *************************************************************************************************************************************
									
-- ============================================================================================================================
/*
                                       TABLE RELATIONSHIPS
								
    ┌─────────────────────────────────────────────────────────────────────────────────────────────┐
    │                          CRM  ←──────────────────────────────────►  ERP                     │
    └─────────────────────────────────────────────────────────────────────────────────────────────┘

    ┌──────────────────────┐        ┌──────────────────────┐        ┌──────────────────────────┐
    │  crm_sales_details   │        │    crm_prd_info      │◄───────│    erp_px_cat_g1v2       │
    │----------------------│        │----------------------│  id=   │--------------------------│
    │  prd_key     ────────┼───────►│  prd_key             │ cat_id │  id                      │
    │  cst_id ───────┐     │        │  cat_id              |        │                          │
    │                │     │        └──────────────────────┘        └──────────────────────────┘
    │                │     │
    │                │     │        ┌──────────────────────┐        ┌──────────────────────────┐
    │                └─────┼───────►│   crm_cust_info      │◄───────│    erp_cust_az12         │
    │                      │        │----------------------│ cst_key│------------------------=-│
    └──────────────────────┘        │  cst_id              │  =cid  │  cid                     │
                                    │  cst_key             │        |                          │
                                    │                      │        └──────────────────────────┘
                                    │                      │
                                    │                      │        ┌──────────────────────────┐
                                    │                      │◄───────│    erp_loc_a101          │
                                    │                      │ cst_key│--------------------------│
                                    └──────────────────────┘  =cid  │  cid                     │
																	│                          │
																	└──────────────────────────┘
*/
-- ============================================================================================================================          


-- Creating the customer Dimension Table
DROP VIEW IF EXISTS gold.dim_customers;
CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_material_status AS material_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN  ci.cst_gndr  -- crm is the Master for gender Info
		 ELSE COALESCE(ca.gen,'n/a')
	END AS gender,
	ca.bdate As birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
ON ci.cst_key = la.cid;

/*----------------------------------------------------------------------------------------------*/
-- checking the gender quality

SELECT DISTINCT
ci.cst_gndr,
ca.gen,
CASE WHEN ci.cst_gndr != 'n/a' THEN  ci.cst_gndr  -- crm is the Master for gender Info
ELSE COALESCE(ca.gen,'n/a')
END AS new_gndr
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.cid;

/*----------------------------------------------------------------------------------------------*/

-- Creating the products Dimension Table
DROP VIEW IF EXISTS gold.dim_products;
CREATE VIEW gold.dim_products AS
SELECT 
ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt,pn.prd_key) AS product_key,
pn.prd_id AS product_id,
pn.prd_key AS product_number,
pn.prd_nm AS product_name,
pn.cat_id AS category_id,
pc.cat AS category,
pc.subcat AS subcategory,
pc.maintenance,
pn.prd_cost AS cost,
pn.prd_line  AS product_line,
pn.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL; -- Filter out all historical data

/*----------------------------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------------------------*/

-- Creating the  Fact Table
DROP VIEW IF EXISTS gold.fact_sales;
CREATE VIEW gold.fact_sales AS
SELECT 
sd.sls_ord_num AS order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS cu
ON sd.sls_cust_id = cu.customer_id; 

/*----------------------------------------------------------------------------------------------*/

-- Foreign key Integrity (Dimension)
SELECT *
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products AS p
ON p.product_key = f.product_key;


SELECT * FROM gold.dim_customers;
SELECT * FROM gold.dim_products;
SELECT * FROM gold.fact_sales;


-- *************************************************************************************************************************************
-- =======================================================*** GOLD LAYER (END) ***======================================================
-- *************************************************************************************************************************************


-- *************************************************************************************************************************************
-- ===============================================*** Explore Data Analysis(EDA) [START] ***============================================
-- *************************************************************************************************************************************


-- ========================== Database Exploration (START) ===========================

SELECT  * FROM INFORMATION_SCHEMA.TABLES;

SELECT *
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('bronze','silver','gold');


SELECT *
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA IN ('bronze','silver','gold');

SELECT *
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = "dim_customers";

-- ========================== Database Exploration (END) =============================



-- ========================== Dimensions Exploration (START) =========================

-- Explore All Countries our customers Comes From
SELECT DISTINCT country FROM gold.dim_customers;

-- Explore All Cetegories "The Major Division"
SELECT DISTINCT category,subcategory,product_name FROM gold.dim_products
ORDER BY 1,2,3;

-- =========================== Dimensions Exploration (END) ==========================



-- ============================ Date Exploration (START) =============================

-- Find the date of first and last date 
-- How many years sales are avaialble
SELECT
MIN(order_date)AS first_order_date ,
MAX(order_date)AS last_order_date ,
TIMESTAMPDIFF(year,MIN(order_date),MAX(order_date)) AS order_range_years
FROM gold.fact_sales; -- So in this case, it returns 3, because 2014-01-28 is less than 4 full years from 2010-12-29.

-- Find the youngest and oldest customer
SELECT 
MIN(birthdate) AS oldest_birthdate,
TIMESTAMPDIFF(year,MIN(birthdate),NOW()) AS oldest_age,
MAX(birthdate) AS youngest_birthdate,
TIMESTAMPDIFF(year,MAX(birthdate),NOW()) AS youngest_age
FROM gold.dim_customers;

-- ============================ Date Exploration (END) ===============================



-- ========================= Measures Exploration (START) ============================

-- Find the Total Sales
SELECT SUM(sales_amount) AS total_sales FROM gold.fact_sales;

-- Find how many items are sold
SELECT SUM(quantity) AS total_quantity FROM gold.fact_sales;

-- Find the average selling price
SELECT ROUND(AVG(price)) AS avg_price FROM gold.fact_sales;

-- Find the Total number of Orders
SELECT COUNT(order_number) AS total_orders FROM gold.fact_sales;
SELECT COUNT( DISTINCT order_number) AS total_orders FROM gold.fact_sales;

-- Find the total number of products
SELECT COUNT(product_name) AS total_products FROM gold.dim_products;
SELECT COUNT(product_key) AS total_products FROM gold.dim_products;

-- Find the total number of customers
SELECT COUNT(customer_id) AS total_customers FROM gold.dim_customers;

-- Find the total number of customers that has placed an order
SELECT COUNT(DISTINCT customer_key) AS total_customers FROM gold.fact_sales;

-- Generate a Report that shows all key metrics of the business 
SELECT 'Total Sales' AS measure_name, SUM(sales_amount) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity' AS measure_name, SUM(quantity) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Avg Price' AS measure_name, ROUND(AVG(price)) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Nr. Orders' AS measure_name, COUNT(DISTINCT order_number) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Nr. Products' AS measure_name, COUNT(product_key) AS measure_value FROM gold.dim_products
UNION ALL
SELECT 'Total Nr. Customers' AS measure_name, COUNT(customer_id) AS total_customers FROM gold.dim_customers;

-- ========================== Measures Exploration (END) =============================



-- ========================== Magnitude Analysis (START) =============================

-- Find total customers by countries
SELECT country,COUNT(customer_id) AS total_customers FROM gold.dim_customers
GROUP BY country
ORDER BY total_customers DESC;

-- Find total customers by gender
SELECT gender,COUNT(customer_id) AS total_customers FROM gold.dim_customers
GROUP BY gender;

-- Find total products by category
SELECT category,COUNT(product_key) AS total_products FROM gold.dim_products
GROUP BY category;

-- What is the average costs in each category?
SELECT category,ROUND(AVG(cost)) AS agv_cost FROM gold.dim_products
GROUP BY category
ORDER BY agv_cost DESC;


-- What is the total revenue generated for each category?
SELECT
p.category,
SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_products AS p
ON p.product_key = f.product_key
GROUP BY p.category
ORDER BY total_revenue DESC;


-- Find total revenue generated by each customer
SELECT
c.customer_key,
c.first_name,
c.last_name,
SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
ON c.customer_key = f.customer_key
GROUP BY c.customer_key,c.first_name,c.last_name
ORDER BY total_revenue DESC;

-- What is the distribution of sold items across countries?
SELECT
c.country,
SUM(f.quantity) AS total_sold_items
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
ON c.customer_key = f.customer_key
GROUP BY c.country
ORDER BY total_sold_items DESC;

-- =========================== Magnitude Analysis (END) ==============================



-- =========================== Ranking Analysis (START) ==============================

-- Which 5 products generate the highest revenue?
SELECT
p.product_name,
SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_products AS p
ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_revenue DESC
LIMIT 5;


-- What are the 5 worst-performing products in terms of sales?
SELECT
p.product_name,
SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_products AS p
ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_revenue
LIMIT 5;

-- Find the top 10 customers who have generated the highest revenue
SELECT
c.customer_key,
c.first_name,
c.last_name,
SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
ON c.customer_key = f.customer_key
GROUP BY c.customer_key,c.first_name,c.last_name
ORDER BY total_revenue DESC
LIMIT 10;

-- The 3 customers with the few orders placed
SELECT
c.customer_key,
c.first_name,
c.last_name,
count(f.order_number) AS total_orders
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
ON c.customer_key = f.customer_key
GROUP BY c.customer_key,c.first_name,c.last_name
ORDER BY total_orders
LIMIT 3;

-- ============================ Ranking Analysis (END) ===============================


-- *************************************************************************************************************************************
-- ===============================================*** Explore Data Analysis(EDA) [END] ***==============================================
-- *************************************************************************************************************************************

