#  Hyderabad Junction: Real-Time AI Train Delay Predictor

## Project Overview
This project is an end-to-end, automated cloud data pipeline that ingests live transit data from the Indian Railways network, processes it through a Medallion architecture, and applies Machine Learning to predict train delays. The final output is a live, interactive Power BI dashboard designed for transit operators and business analysts to monitor network health.

**Live Dashboard Preview:**
<img width="1043" height="589" alt="dashboard_preview" src="https://github.com/user-attachments/assets/e9c29036-8b32-4a1b-ae13-c246e777ddde" />


##  Cloud Architecture & Tech Stack
* **Data Source:** RapidAPI (Live Indian Railways endpoints)
* **Cloud Storage:** Azure Data Lake Storage Gen2 (ADLS)
* **Compute & Engineering:** Databricks (PySpark, Python)
* **Machine Learning:** Databricks MLlib (Random Forest Classifier)
* **Visualization:** Power BI (DAX, Data Modeling)

##  The Data Pipeline (Medallion Architecture)
1. **Ingestion (Bronze Layer):** A Python-based API caller connects to live railway endpoints. Engineered custom throttling logic to navigate strict API rate limits. Raw, deeply nested JSON data is dumped directly into Azure Data Lake.
<img width="913" height="699" alt="ingestion and auto-loader setup" src="https://github.com/user-attachments/assets/0dac7d16-2697-45ba-9e84-a55d7e3b40df" />

3. **Transformation (Silver Layer):** PySpark is utilized to flatten the nested JSON, enforce schema constraints, handle null values, and clean timestamps. The refined data is saved as optimized Delta tables.
4. **Machine Learning (Gold Layer):** Engineered features based on `train_type`, `station_role`, and historical delay patterns. Trained a Random Forest model to predict the likelihood of a delay (0 = On Time, 1 = Delayed).
5. <img width="909" height="806" alt="ml-training" src="https://github.com/user-attachments/assets/ebbd0ddb-3270-41f7-8a84-4760fdc09f63" />

6. **Serving (Power BI):** Bypassed strict Databricks Unity Catalog security limitations by engineering an internal Managed Table to serve as a direct, secure data feed to Power BI via a Personal Access Token (PAT).

##  Key Business Metrics Tracked (DAX)
* **Total Trains Monitored:** `COUNTROWS(Distinct)`
* **Predicted Delays:** Real-time summation of the ML prediction output.
* **Network Delay Rate %:** A custom DAX measure calculating `(Total Delays / Total Trains) * 100` to provide immediate contextual health of the transit grid.

