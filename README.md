# Flight Booking Data Pipeline 🚀

## Overview
End‑to‑end pipeline for flight booking analytics using:
- **Airflow (Composer)** for orchestration  
- **Dataproc Serverless (PySpark)** for transformations  
- **Google Cloud Storage (GCS)** for staging data  
- **BigQuery** for analytics  
- **GitHub Actions** for CI/CD  

Raw flight booking data (CSV) is ingested, transformed with PySpark, and loaded into BigQuery tables.

---

## 📊 Simple Architecture
```mermaid
flowchart LR
    GitHubDev[GitHub - dev branch] --> AirflowDev[Airflow - Dev]
    GitHubMain[GitHub - main branch] --> AirflowProd[Airflow - Prod]

    AirflowDev --> Dataproc[Dataproc Serverless]
    AirflowProd --> Dataproc

    Dataproc --> BigQueryDev[BigQuery - flight_data_dev]
    Dataproc --> BigQueryProd[BigQuery - flight_data_prod]

    GCS[flight_booking.csv in GCS] --> Dataproc



   
