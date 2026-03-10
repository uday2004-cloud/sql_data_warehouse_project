-- ========================================================================================================================================
/*
											DATA WAREHOUSE MEDALLION ARCHITECTURE
                            
						+------------------------+     +------------------------+     +------------------------+
						|       BRONZE LAYER     |     |       SILVER LAYER     |     |        GOLD LAYER      |
						|------------------------|     |------------------------|     |------------------------|
						| Raw Data Ingestion     |     | Data Cleaning          |     | Business Data Model    |
						|                        |     |                        |     |                        |
						| • Source CSV Files     | --> | • Remove Duplicates    | --> | • Star Schema          |
						| • CRM Data             |     | • Handle NULL Values   |     | • Dimension Tables     |
						| • ERP Data             |     | • Standardization      |     | • Fact Table           |
						| • No Transformations   |     | • Data Validation      |     | • Analytics Ready      |
						+------------------------+     +------------------------+     +------------------------+

*/
-- ========================================================================================================================================

-- *************************************************************************************************************************************
-- =====================================================*** BRONZE LAYER (START) ***====================================================
-- *************************************************************************************************************************************

/*---------Creating a database "DataWarehouse"---------*/
DROP DATABASE IF EXISTS DataWarehouse;

CREATE DATABASE DataWarehouse;
/*---------Changing a database "DataWarehouse"---------*/

USE DataWarehouse;

/*---------if exISts Schema drop and Create a database "bronze"---------*/

DROP SCHEMA IF EXISTS bronze;
CREATE SCHEMA bronze;

/*---------Creating a database "bronze" if not exISts---------*/

DROP SCHEMA IF EXISTS silver;
CREATE SCHEMA silver;

DROP SCHEMA IF EXISTS gold;
CREATE SCHEMA gold;

DROP TABLE IF EXISTS bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info(
cst_id INT,
cst_key VARCHAR(50),
cst_firstname VARCHAR(50),
cst_lastname VARCHAR(50),
cst_material_status VARCHAR(50),
cst_gndr VARCHAR(50),
cst_create_date DATE
);

DROP TABLE IF EXISTS bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info(
prd_id INT,
prd_key VARCHAR(50),
prd_nm VARCHAR(50),
prd_cost INT,
prd_line VARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE
);

DROP TABLE IF EXISTS bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details(
sls_ord_num VARCHAR(50),
sls_prd_key VARCHAR(50),
sls_cust_id      INT,
sls_ORDER_dt     INT,
sls_ship_dt   	INT,
sls_due_dt	   INT,
sls_sales       	INT,
sls_quantity   INT,
sls_price INT
);

DROP TABLE IF EXISTS bronze.erp_loc_a101;

CREATE TABLE bronze.erp_loc_a101(
cid VARCHAR(50),
cntry VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_cust_az12;

CREATE TABLE bronze.erp_cust_az12(
cid VARCHAR(50),
bdate DATE,
gen VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;

CREATE TABLE bronze.erp_px_cat_g1v2(
id VARCHAR(50),
cat VARCHAR(50),
subcat VARCHAR(50),
maintenance VARCHAR(50)
);

/*-------------------------------------------------------------------------------------------------------------------------------------

												LOADIND THE RAW DATA IN DATABASE

--------------------------------------------------------------------------------------------------------------------------------------*/


/*--------------------------------------------------------------------------------------------------------
Edit thIS line in the windows to loada
[mysqld]
secure-file-priv=""
-----------------------------------------
And in MAC:

Start MySQL with:                             ----------LOAD THE DATA-----------
mysql --local-infile=1 -u root -p
-------------------------------------         ----- DROP PROCEDURE IF EXISTS bronze_load_bronze;
													DELIMITER //
													CREATE PROCEDURE bronze.load_bronze()
													BEGIN
                                                    ---code
                                                    END
                                                    DELIMITER;-----
                                                    CALL bronze.load_bronze();---TO CALL 
                                                    
SHOW VARIABLES LIKE 'secure_file_priv';
SET GLOBAL secure_file_priv = '';
SET GLOBAL local_infile = 1;
SHOW VARIABLES LIKE 'local_infile';

SHOW VARIABLES LIKE 'local_infile';


---------------------------------------------------------------------------------------------------------*/
SET @Start_Time = now();
SELECT @Start_Time AS Start_time;
TRUNCATE TABLE bronze.crm_cust_info;
LOAD DATA LOCAL INFILE '/Users/uday/Downloads/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
INTO TABLE bronze.crm_cust_info
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'  
IGNORE 1 ROWS
(cst_id,cst_key,cst_firstname,cst_lastname,cst_material_status,cst_gndr,@cst_create_date) 
SET 
cst_id = NULLIF(cst_id, ''),
cst_key = NULLIF(cst_key, ''),
cst_firstname = NULLIF(cst_firstname, ''),
cst_lastname = NULLIF(cst_lastname, ''),
cst_material_status = NULLIF(cst_material_status, ''),
cst_gndr = NULLIF(cst_gndr, ''),
cst_create_date = CASE 
      WHEN @cst_create_date = '' OR @cst_create_date = '0000-00-00' OR @cst_create_date IS NULL 
      THEN NULL 
      ELSE STR_TO_DATE(@cst_create_date, '%Y-%m-%d')  
  END;
  
  
  TRUNCATE TABLE bronze.crm_prd_info;
LOAD DATA LOCAL INFILE '/Users/uday/Downloads/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
INTO TABLE bronze.crm_prd_info
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'         
IGNORE 1 ROWS
(prd_id, prd_key, prd_nm, @prd_cost, prd_line, @prd_start_dt, @prd_end_dt)
SET
  prd_id         = NULLIF(prd_id, ''),
  prd_key        = NULLIF(prd_key, ''),
  prd_nm         = NULLIF(prd_nm, ''),
  prd_cost       = NULLIF(@prd_cost, ''),          
  prd_line       = NULLIF(prd_line, ''),
  prd_start_dt   = CASE 
                     WHEN @prd_start_dt = '' OR @prd_start_dt IS NULL THEN NULL
                     ELSE STR_TO_DATE(@prd_start_dt, '%Y-%m-%d')
                   END,
  prd_end_dt     = CASE 
                     WHEN @prd_end_dt = '' OR @prd_end_dt IS NULL THEN NULL
                     ELSE STR_TO_DATE(@prd_end_dt, '%Y-%m-%d')
                   END;
                   
    TRUNCATE TABLE bronze.crm_sales_details;
LOAD DATA LOCAL INFILE '/Users/uday/Downloads/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
INTO TABLE bronze.crm_sales_details
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'          
IGNORE 1 ROWS
(sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, @sls_sales, sls_quantity, @sls_price)
SET
  sls_ord_num  = NULLIF(sls_ord_num, ''),
  sls_prd_key  = NULLIF(sls_prd_key, ''),
  sls_cust_id  = NULLIF(sls_cust_id, ''),
  sls_order_dt = NULLIF(sls_order_dt, ''),
  sls_ship_dt  = NULLIF(sls_ship_dt, ''),
  sls_due_dt   = NULLIF(sls_due_dt, ''),
  sls_sales    = NULLIF(@sls_sales, ''),                      
  sls_price    = NULLIF(@sls_price, '');                      
  
  
  

TRUNCATE TABLE bronze.erp_px_cat_g1v2;
LOAD DATA LOCAL INFILE '/Users/uday/Downloads/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
INTO TABLE bronze.erp_px_cat_g1v2
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'      
IGNORE 1 ROWS
(ID, CAT, SUBCAT, MAINTENANCE)
SET
  ID          = NULLIF(ID, ''),
  CAT         = NULLIF(CAT, ''),
  SUBCAT      = NULLIF(SUBCAT, ''),
  MAINTENANCE = NULLIF(MAINTENANCE, '');



TRUNCATE TABLE bronze.erp_loc_a101;
LOAD DATA LOCAL INFILE '/Users/uday/Downloads/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
INTO TABLE bronze.erp_loc_a101
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'        
IGNORE 1 ROWS
(CID, CNTRY)
SET
  CID   = NULLIF(CID, ''),
  CNTRY = NULLIF(CNTRY, '');     



TRUNCATE TABLE bronze.erp_cust_az12;
LOAD DATA LOCAL INFILE '/Users/uday/Downloads/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
INTO TABLE bronze.erp_cust_az12
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'          
IGNORE 1 ROWS
(CID, @BDATE, GEN)
SET
  CID   = NULLIF(CID, ''),
  BDATE = CASE
            WHEN @BDATE = '' OR @BDATE IS NULL THEN NULL
            ELSE STR_TO_DATE(@BDATE, '%Y-%m-%d')  
          END,
  GEN   = NULLIF(GEN, '');          

SET @End_Time = NOW();
SELECT @End_Time AS End_time;

SELECT * FROM bronze.crm_cust_info;
SELECT * FROM bronze.crm_prd_info;
SELECT * FROM bronze.crm_sales_details;
SELECT * FROM bronze.erp_px_cat_g1v2;
SELECT * FROM bronze.erp_loc_a101;
SELECT * FROM bronze.erp_cust_az12;

-- *************************************************************************************************************************************
-- ======================================================*** BRONZE LAYER (END) ***=====================================================
-- *************************************************************************************************************************************

