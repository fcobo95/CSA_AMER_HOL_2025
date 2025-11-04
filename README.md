# Cloudera on Azure — Hands-on Laboratory Overview

## Objective
This lab provides a practical overview of building a complete data pipeline using the **Cloudera Data Platform (CDP) on Azure**. You will move data from Azure Data Lake Storage (ADLS) through **Cloudera Data Flow (CDF)** and **Stream Messaging (Kafka)**, store it in **Cloudera Data Warehouse (CDW)** as Iceberg tables, and explore it using **Cloudera Artificial Intelligence (CAI)**.

---

## Step 1 - Create your schema in Kafka Schema Registry
- Use the file called `kafka_avro_schema.avro` to create your first Kafka Schema Registry schema. 
- Verify schema is properly configured and is called `telco_data_{CSA_ALIAS}`.

## Step 2 — Ingest Data from ADLS to Kafka (Cloudera Data Flow)
- Import a flow in **CDF Catalog**.  
- Read a `.csv` file from **Azure Data Lake Storage (ADLS)**.  
- Convert the file’s records into message format.  
- Publish the messages to a **Kafka topic** in a **Stream Messaging Data Hub Cluster**.  
- Verify messages are flowing correctly into Kafka.

---

## Step 3 — Prepare a Database and Iceberg Table (Cloudera Data Warehouse)
- Open the **Cloudera Data Warehouse (CDW)** service.  
- Use the **default Hive Virtual Warehouse**.  
- Create a new **database** for the lab.  
- Create an empty **Iceberg table** to hold the incoming data (no data yet).

---

## Step 4 — Consume Kafka Messages into Iceberg (CDF to CDW)
- Create a second flow in **CDF**.  
- Consume messages from the Kafka topic created in Step 1.  
- Write those messages into the **Iceberg table** in the **CDW Hive Virtual Warehouse**.  
- Confirm data appears correctly in the table.

---

## Step 5 — Explore and Analyze Data (Cloudera Machine Learning)
- Open **Cloudera Machine Learning (CML)**.  
- Create a new project to connect to the **CDW** database.  
- Load and explore the Iceberg data set.  
- Optionally run simple **AI or ML workflows** (e.g., data profiling, regression, or visualization) on the stored dataset.

---

## Summary
You have completed an end-to-end data pipeline using **Cloudera on Azure**:
1. Configure your **Kafka** topic schema.
1. Ingested data from **ADLS** to **Kafka** using **CDF**.  
1. Prepared an **Iceberg table** in **CDW**.  
1. Streamed Kafka messages into **CDW** as structured Iceberg data.  
1. Explored and analyzed the dataset in **CML**.

This workflow demonstrates how **Cloudera Data Platform** enables a unified, cloud-native data lifecycle on **Microsoft Azure** — from ingestion to AI-driven insights.
