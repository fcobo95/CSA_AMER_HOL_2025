/* ==========================================
+.  Authors -      Fernando Cobo, Dongkai Yu
+.  Roles -       Cloud Solutions Architects
+.  Date created: 2025-10-28
+.  SQL Scripts for CSA Hands-on Lab 2025
========================================== */

-- Step 1. Drop the database before the Hands-On lab in case one with your alias already exists in the default-hive-az Virtual Warehouse.
DROP DATABASE IF EXISTS ${CSA_ALIAS};

-- Step 2. Create the database with your name as the db name in the default-hive-az Virtual Warehouse. 
CREATE DATABASE ${CSA_ALIAS};

-- Step 3. Create the Iceberg table in the default-hive-az Virtual Warehouse.
CREATE EXTERNAL TABLE ${CSA_ALIAS}.telco_iceberg_kafka (
  `multiplelines` string,
  `paperlessbilling` string,
  `gender` string,
  `onlinesecurity` string,
  `internetservice` string,
  `techsupport` string,
  `contract` string,
  `churn` string,
  `seniorcitizen` string,
  `deviceprotection` string,
  `streamingtv` string,
  `streamingmovies` string,
  `totalcharges` string,
  `partner` string,
  `monthlycharges` string,
  `customerid` string,
  `dependents` string,
  `onlinebackup` string,
  `phoneservice` string,
  `tenure` string,
  `paymentmethod` string
)
STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler'
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'engine.hive.enabled'='true',
  'iceberg.catalog.type'='hive',
  'write.format.default'='parquet'  -- Or 'avro' or 'orc', depending on your choice
);

-- Calculate churn rate by contract type to determine if a customer's contract length is a strong predictor of churn
SELECT
  Contract,
  COUNT(*) AS total_customers,
  SUM(CASE WHEN Churn = 'Yes' THEN 1 ELSE 0 END) AS churned_customers,
  CAST(SUM(CASE WHEN Churn = 'Yes' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS churn_rate
FROM ${CSA_ALIAS}.telco_iceberg_kafka
GROUP BY Contract
ORDER BY churn_rate DESC;

-- Compare average monthly and total charges for churned versus non-churned customers to see if spending habits correlate with churn.
SELECT
  Churn,
  AVG(CAST(MonthlyCharges AS DOUBLE)) AS avg_monthly_charges,
  AVG(CAST(TotalCharges AS DOUBLE)) AS avg_total_charges
FROM ${CSA_ALIAS}.telco_iceberg_kafka
GROUP BY Churn;

-- Analyze churn rate by customer tenure by grouping customers into tenure bins. This helps visualize if newer customers churn more frequently.
SELECT
  CASE
    WHEN CAST(tenure AS INT) <= 12 THEN '0-12 months'
    WHEN CAST(tenure AS INT) > 12 AND CAST(tenure AS INT) <= 24 THEN '13-24 months'
    WHEN CAST(tenure AS INT) > 24 AND CAST(tenure AS INT) <= 48 THEN '25-48 months'
    WHEN CAST(tenure AS INT) > 48 THEN '49+ months'
    ELSE 'Unknown'
  END AS tenure_group,
  COUNT(*) AS total_customers,
  SUM(CASE WHEN Churn = 'Yes' THEN 1 ELSE 0 END) AS churned_customers,
  CAST(SUM(CASE WHEN Churn = 'Yes' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS churn_rate
FROM ${CSA_ALIAS}.telco_iceberg_kafka
GROUP BY
  CASE
    WHEN CAST(tenure AS INT) <= 12 THEN '0-12 months'
    WHEN CAST(tenure AS INT) > 12 AND CAST(tenure AS INT) <= 24 THEN '13-24 months'
    WHEN CAST(tenure AS INT) > 24 AND CAST(tenure AS INT) <= 48 THEN '25-48 months'
    WHEN CAST(tenure AS INT) > 48 THEN '49+ months'
    ELSE 'Unknown'
  END
ORDER BY tenure_group;