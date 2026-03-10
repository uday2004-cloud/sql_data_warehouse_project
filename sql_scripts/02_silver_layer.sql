

-- *************************************************************************************************************************************
-- =====================================================*** SILVER LAYER (START) ***====================================================
-- *************************************************************************************************************************************

/*-------------------------------------------------------------------------------------------------------------------------------------

								CLEANING & STANDARDLIZING THE DATA AND INSERTING INTO SILVER LAYER

--------------------------------------------------------------------------------------------------------------------------------------*/


DROP TABLE IF EXISTS silver.crm_cust_info;

CREATE TABLE silver.crm_cust_info(
cst_id INT,
cst_key VARCHAR(50),
cst_firstname VARCHAR(50),
cst_lastname VARCHAR(50),
cst_material_status VARCHAR(50),
cst_gndr VARCHAR(50),
cst_create_date DATE,
dwh_create_date DATETIME DEFAULT now()
);

DROP TABLE IF EXISTS silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info(
prd_id INT,
cat_id VARCHAR(50),
prd_key VARCHAR(50),
prd_nm VARCHAR(50),
prd_cost INT,
prd_line VARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE,
dwh_create_date DATETIME DEFAULT now()
);

DROP TABLE IF EXISTS silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details(
sls_ord_num VARCHAR(50),
sls_prd_key VARCHAR(50),
sls_cust_id INT,
sls_ORDER_dt    DATE,
sls_ship_dt	    DATE,
sls_due_dt	  DATE,
sls_sales	  INT,
sls_quantity  INT,
sls_price INT,
dwh_create_date DATETIME DEFAULT now()
);

DROP TABLE IF EXISTS silver.erp_loc_a101;

CREATE TABLE silver.erp_loc_a101(
cid VARCHAR(50),
cntry VARCHAR(50),
dwh_create_date DATETIME DEFAULT now()
);

DROP TABLE IF EXISTS silver.erp_cust_az12;

CREATE TABLE silver.erp_cust_az12(
cid VARCHAR(50),
bdate DATE,
gen VARCHAR(50),
dwh_create_date DATETIME DEFAULT now()
);

DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2(
id VARCHAR(50),
cat VARCHAR(50),
subcat VARCHAR(50),
maintenance VARCHAR(50),
dwh_create_date DATETIME DEFAULT now()
);

/*--------------------------------------------------------------------------------------------------------------

						 INSERTING THE CLEANED DATA INTO SILVER LAYER (DATA TRANSFORMING)

----------------------------------------------------------------------------------------------------------------*/

/*-------------------------------------------------------------------------------------------------------------- 
---------------------------- Checking the crm_cst_info data before inserting  ----------------------------------
----------------------------------------------------------------------------------------------------------------*/
-- Checking fOR NULLs OR Dulicates in primary key
-- Expectation: No Result

SELECT cst_id,count(*) FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING count(*) > 1 OR cst_id IS NULL;

SELECT * FROM ( SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
) t  WHERE flag_last = 1;

-- Checking fOR Unwanted Spaces
-- Expectation: No Result

SELECT cst_firstname,cst_lastname FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname) OR cst_lastname != TRIM(cst_lastname);

/*-----------------------------------------------------------------------------------------------------------------------
						Transforming data into crm_cust_info (silver layer)
------------------------------------------------------------------------------------------------------------------------*/
TRUNCATE TABLE silver.crm_cust_info;
INSERT INTO silver.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_material_status,
cst_gndr,
cst_create_date)
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
	 WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
     ELSE 'n/a'
END cst_material_status,     -- NORmalize the Material_status values to readable fORmate

CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
     ELSE 'n/a'
END cst_gndr,  -- NORmalize the gender values to readable fORmate
cst_create_date
FROM (
SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
) t  WHERE flag_last = 1;


/*-------------------------------------------------------------------------------------------------------------- 
---------------------------- Checking the crm_cst_info data after inserting  ----------------------------------
----------------------------------------------------------------------------------------------------------------*/
-- Checking fOR NULLs OR Dulicates in primary key
-- Expectation: No Result

SELECT cst_id,count(*) FROM silver.crm_cust_info
GROUP BY cst_id
HAVING count(*) > 1 OR cst_id IS NULL;

SELECT * FROM ( SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM silver.crm_cust_info
WHERE cst_id IS NOT NULL
) t  WHERE flag_last = 1;

-- Checking fOR Unwanted Spaces
-- Expectation: No Result

SELECT cst_firstname,cst_lastname FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname) OR cst_lastname != TRIM(cst_lastname);

-- Data Standardzation & ConsIStency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT * FROM silver.crm_cust_info;


/*-------------------------------------------------------------------------------------------------------------- 
---------------------------- Checking the crm_prd_info data before inserting  ----------------------------------
----------------------------------------------------------------------------------------------------------------*/

-- Check fOR NULLs OR duplicates in primary key 
-- Expectaion: No Results

SELECT prd_id,count(*) FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING count(*) > 1 OR prd_id IS NULL;

-- Checking fOR Unwanted Spaces
-- Expectation: No Result
 
SELECT prd_nm FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check fOR NULLs OR Negative values
-- Expectaion: No Results

SELECT prd_cost FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data Standardization & ConsIStency
SELECT DISTINCT prd_line FROM bronze.crm_prd_info;

-- Checking invalid Data ORDERs
SELECT * FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2; -- it contain underscORe 
/*--------------------------------------------------------------------------------------------------------------
						Transforming data into crm_prd_info (silver layer)
---------------------------------------------------------------------------------------------------------------*/
INSERT INTO silver.crm_prd_info(
prd_id,
cat_id,
prd_key, 
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt)
SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key,7,LENGTH(prd_key)) AS prd_key,
prd_nm,
ifNULL(prd_cost,0) AS prd_cost,
CASE	UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mountain'
    WHEN 'R' THEN 'Road'
    WHEN 'S' THEN 'Other Sales'
    WHEN 'T' THEN 'Touring'
    ELSE 'n/a'
END AS prd_line,
prd_start_dt,
DATE_SUB(lead(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt), INTERVAL 1 DAY) AS prd_end_dt
FROM bronze.crm_prd_info;
 
 SELECT * FROM silver.crm_prd_info;
 
 
/*-------------------------------------------------------------------------------------------------------------- 
---------------------------- Checking the crm_prd_info data after inserting  ----------------------------------
----------------------------------------------------------------------------------------------------------------*/

-- Check fOR NULLs OR duplicates in primary key 
-- Expectaion: No Results

SELECT prd_id,count(*) FROM silver.crm_prd_info
GROUP BY prd_id
HAVING count(*) > 1 OR prd_id IS NULL;

-- Checking fOR Unwanted Spaces
-- Expectation: No Result
 
SELECT prd_nm FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check fOR NULLs OR Negative values
-- Expectaion: No Results

SELECT prd_cost FROM silver.crm_prd_info
WHERE prd_cost < 0 ;

-- Data Standardization & ConsIStency
SELECT DISTINCT prd_line FROM silver.crm_prd_info;

-- Checking invalid Data ORDERs
SELECT * FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

SELECT * FROM silver.crm_prd_info;

/*-------------------------------------------------------------------------------------------------------------- 
---------------------------- Checking the crm_sales_details data before inserting  -----------------------------
----------------------------------------------------------------------------------------------------------------*/

-- checking invaild dates for sls_order_dt
SELECT 
NULLIF(sls_order_dt,0) AS sls_order_dt FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LENGTH(sls_order_dt) != 8 OR sls_order_dt > 20500101 OR sls_order_dt < 19000101;

-- checking invaild dates for sls_ship_dt
SELECT 
NULLIF(sls_ship_dt,0) AS sls_ship_dt FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 OR LENGTH(sls_ship_dt) != 8 OR sls_ship_dt > 20500101 OR sls_ship_dt < 19000101;

-- checking invaild dates for sls_due_dt
SELECT 
NULLIF(sls_due_dt,0) AS sls_due_dt FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 OR LENGTH(sls_due_dt) != 8 OR sls_due_dt > 20500101 OR sls_due_dt < 19000101;

-- Check for Invalid Date Orders
SELECT * FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt  OR sls_order_dt > sls_due_dt;

-- checking data conistncy: Between Sales, Quantity, and Price 
-- >> Sales = Quantity * Price 
-- >> Values must not be null, Zero, or negtive.

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0  OR sls_price <=0 
ORDER BY sls_sales,sls_quantity,sls_price;

/*-------------------------------------------------------------------------------------------------------
						TransfORming data into crm_sales_details (silver layer)
--------------------------------------------------------------------------------------------------------*/
INSERT INTO silver.crm_sales_details(
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
)
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt <= 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
	 ELSE CAST(sls_order_dt AS DATE)
END AS sls_order_dt,
CASE WHEN sls_ship_dt <= 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
	 ELSE CAST(sls_ship_dt AS DATE)
END AS sls_ship_dt,
CASE WHEN sls_due_dt <= 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
	 ELSE CAST(sls_due_dt AS DATE)
END AS sls_due_dt,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN  sls_quantity * ABS(sls_price)
	 ELSE sls_sales
END AS sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN round( sls_sales/ NULLIF(sls_quantity,0))
	 ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details;

/*-------------------------------------------------------------------------------------------------------------- 
---------------------------- Checking the crm_sales_details data after inserting  -----------------------------
----------------------------------------------------------------------------------------------------------------*/

-- Check for Invalid Date Orders
SELECT * FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt  OR sls_order_dt > sls_due_dt;

-- checking data conistncy: Between Sales, Quantity, and Price 
-- >> Sales = Quantity * Price 
-- >> Values must not be null, Zero, or negtive.
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0  OR sls_price <=0 
ORDER BY sls_sales,sls_quantity,sls_price;

SELECT * FROM silver.crm_sales_details;

/*-------------------------------------------------------------------------------------------------------------- 
---------------------------- Checking the erp_cust_az12 data before inserting  -----------------------------
----------------------------------------------------------------------------------------------------------------*/
-- Identify Out-Of-Range Date
SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > NOW();

/*--- UPDATE bronze.erp_cust_az12										--if needed
      SET gen = REPLACE(REPLACE(gen, '\r', ''), '\n', '') ---*/

-- Data Standardization & Consistency
SELECT DISTINCT
gen
FROM bronze.erp_cust_az12;
/*-------------------------------------------------------------------------------------------------------
						Transforming data into erp_cust_az12 (silver layer)
--------------------------------------------------------------------------------------------------------*/
INSERT INto silver.erp_cust_az12(
cid,
bdate,
gen
)
SELECT
CASE WHEN cid like "NAS%" THEN SUBSTRING(cid,4,LENGTH(cid))
	 ELSE cid
END AS cid,
CASE WHEN bdate > NOW() THEN NULL
	 ELSE bdate
END AS bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12;
/*-------------------------------------------------------------------------------------------------------------- 
---------------------------- Checking the erp_cust_az12 data before inserting  -----------------------------
----------------------------------------------------------------------------------------------------------------*/
-- Identify Out-Of-Range Date
SELECT DISTINCT
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > NOW();   -- future dates only removed

-- Data Standardization & Consistency
SELECT DISTINCT
gen
FROM silver.erp_cust_az12;

SELECT * FROM silver.erp_cust_az12;

/*-------------------------------------------------------------------------------------------------------------- 
---------------------------- Checking the erp_loc_a101 data before inserting  -----------------------------
----------------------------------------------------------------------------------------------------------------*/
SELECT cst_key FROM silver.crm_cust_info; -- no hyphen

/*--- UPDATE bronze.erp_loc_a101										--if needed
      SET cntry = REPLACE(REPLACE(cntry, '\r', ''), '\n', '') ---*/
      

-- Data Standardization & Consistency
SELECT DISTINCT
cntry,
CASE
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
    WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
    WHEN TRIM(cntry) = '' or TRIM(cntry) IS NULL THEN 'n/a'
    ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_loc_a101;


/*-------------------------------------------------------------------------------------------------------
						Transforming data into erp_loc_a101 (silver layer)
--------------------------------------------------------------------------------------------------------*/
INSERT INTO silver.erp_loc_a101(
cid,
cntry
)
SELECT 
REPLACE(CID,'-','')cid,
CASE
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
    WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
    WHEN TRIM(cntry) = '' or TRIM(cntry) IS NULL THEN 'n/a'
    ELSE TRIM(cntry)
    END AS cntry		-- Normalize and Handle missing or blank ountry codes
FROM bronze.erp_loc_a101;

/*-------------------------------------------------------------------------------------------------------
						Transforming data into erp_loc_a101 (silver layer)
--------------------------------------------------------------------------------------------------------*/
-- Data Standardization & Consistency
SELECT DISTINCT
cntry
FROM silver.erp_loc_a101;

SELECT * FROM silver.erp_loc_a101;

/*-------------------------------------------------------------------------------------------------------------- 
---------------------------- Checking the erp_px_cat_g1v2 data before inserting  -----------------------------
----------------------------------------------------------------------------------------------------------------*/
-- Check for unwanted Spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

/*--- UPDATE bronze.erp_px_cat_g1v2               -- if needed
	  SET maintenance = REPLACE(REPLACE(maintenance, '\r', ''), '\n', ''); ---*/

-- Data Standardization & Consistency
SELECT DISTINCT
id
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT
cat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT
subcat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT
maintenance
FROM bronze.erp_px_cat_g1v2;

/*-------------------------------------------------------------------------------------------------------
						Transforming data into erp_px_cat_g1v2 (silver layer)
--------------------------------------------------------------------------------------------------------*/
INSERT INTO silver.erp_px_cat_g1v2(
id,
cat,
subcat,
maintenance
)
SELECT 
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2;
				
/*-------------------------------------------------------------------------------------------------------------- 
---------------------------- Checking the erp_px_cat_g1v2 data after inserting  -----------------------------
----------------------------------------------------------------------------------------------------------------*/
-- No Modifications
SELECT * FROM silver.erp_px_cat_g1v2;


-- *************************************************************************************************************************************
-- ======================================================*** SILVER LAYER (END) ***=====================================================
-- *************************************************************************************************************************************

