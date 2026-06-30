This repository contains an end‑to‑end data engineering pipeline for flight booking analytics, built on:

Apache Airflow (Composer) for orchestration

Dataproc Serverless (PySpark) for transformations

Google Cloud Storage (GCS) for staging data

BigQuery for analytics and reporting

GitHub Actions for CI/CD deployment

The pipeline ingests raw flight booking data (CSV), transforms it using PySpark, and loads results into BigQuery tables for downstream insights.

Architecture Diagram

flowchart TD
    subgraph Dev[Development Flow]
        A[Local Dev] --> B[Push to dev branch]
        B --> C[GitHub Actions: upload-to-dev]
        C --> D[Airflow-Dev Environment]
        D --> E[GCS Bucket (dev)]
        E --> F[Dataproc Serverless]
        F --> G[BigQuery Dataset: flight_data_dev]
    end

    subgraph Prod[Production Flow]
        H[Merge dev → main] --> I[Push to main branch]
        I --> J[GitHub Actions: upload-to-prod]
        J --> K[Airflow-Prod Environment]
        K --> L[GCS Bucket (prod)]
        L --> F
        F --> M[BigQuery Dataset: flight_data_prod]
    end

    subgraph Input[Raw Data]
    
Repo Structure

.github/workflows/   # CI/CD pipelines
airflow_job/         # Airflow DAGs
spark_job/           # PySpark jobs
variables/           # Config JSONs (dev/prod)
flight_booking.csv   # Sample input
