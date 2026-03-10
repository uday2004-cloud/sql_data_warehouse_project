# sql_data_warehouse_project
End-to-End SQL Data Warehouse and Analytics project implementing Medallion Architecture (Bronze, Silver, Gold) to build a scalable data pipeline from raw CRM and ERP data to analytics-ready datasets and business insights.

## 📌 Project Overview

This project builds a complete **Data Warehouse** from raw CSV data using the **Medallion Architecture** (Bronze → Silver → Gold) in MySQL.

It covers the full data engineering and analytics pipeline:
- Raw data ingestion
- Data cleaning & transformation
- Dimensional modeling (Star Schema)
- Exploratory Data Analysis (EDA)
- Advanced Analytics & Business Reporting

---

## 🏛️ Architecture
```
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
```
---

## 🗂️ Data Sources

|   Source     |                         Tables                               |
|--------------|--------------------------------------------------------------|
| `CRM System` | Customer Info, Product Info, Sales Details                   |
|`ERP System ` | Customer Demographics, Customer Location, Product Categories |

---

## 🗃️ Repository Structure
```
data-warehouse-project/
│
├── sql/
│   └── Untitled.sql          # Full pipeline: Bronze → Silver → Gold → Analytics
│
├── datasets/
│   ├── source_crm/
│   │   ├── cust_info.csv
│   │   ├── prd_info.csv
│   │   └── sales_details.csv
│   └── source_erp/
│       ├── CUST_AZ12.csv
│       ├── LOC_A101.csv
│       └── PX_CAT_G1V2.csv
│
└── README.md
```
## 🧱 Data Model — Star Schema

```
                    ┌─────────────────┐
                    │  dim_customers  │
                    └────────┬────────┘
                             │
┌─────────────────┐   ┌──────┴───────┐  
│  dim_products   ├───┤  fact_sales  |
└─────────────────┘   └──────────────┘   
```

|         Table        |   Type    |                   Description                     |
|----------------------|-----------|---------------------------------------------------|
| `gold.dim_customers` | Dimension | Customer details, demographics, location          |
| `gold.dim_products`  | Dimension | Product info, category, cost                      |
| `gold.fact_sales`    | Fact      | Sales transactions linked to customers & products |

---

## 🔍 What's Inside the SQL

### 🥉 Bronze Layer
- Creates raw tables mirroring CSV source files
- Loads data with NULL handling and date parsing

### 🥈 Silver Layer
- Removes duplicates using `ROW_NUMBER()`
- Trims whitespace from string fields
- Standardizes codes (e.g., `'M'` → `'Male'`, `'S'` → `'Single'`, `'DE'` → `'Germany'`)
- Validates and corrects sales figures (`sales = quantity × price`)
- Derives new columns (e.g., `cat_id` from `prd_key`, computed `prd_end_dt` via `LEAD()`)

### 🥇 Gold Layer
- Builds `dim_customers`, `dim_products`, and `fact_sales` as views
- Implements Star Schema for analytics

### 📊 EDA & Advanced Analytics
- Sales trends over time
- Running totals & moving averages (window functions)
- Year-over-year product performance using `LAG()`
- Category contribution (part-to-whole analysis)
- Customer segmentation: **VIP / Regular / New**
- Product segmentation: **High-Performer / Mid-Range / Low-Performer**

### 📋 Business Reports
|          Report         |                           Description                            |
|-------------------------|------------------------------------------------------------------|
| `gold.report_customers` | Customer KPIs: recency, AOV, monthly spend, age group, segment   |
| `gold.report_products`  | Product KPIs: recency, AOR, monthly revenue, performance segment |

---

## 🛠️ Skills Demonstrated

- ETL Pipeline Design
- Data Cleaning & Standardization
- Dimensional Modeling (Star Schema)
- SQL Window Functions (`ROW_NUMBER`, `LAG`, `LEAD`, `SUM OVER`)
- Common Table Expressions (CTEs)
- Exploratory Data Analysis (EDA)
- Business KPI Reporting

---

## 👤 Author

**Uday**  
Aspiring Data Analyst  
📧 udaybontha1998@gmail.com
🔗 [LinkedIn](https://www.linkedin.com/in/udaybontha)

---
