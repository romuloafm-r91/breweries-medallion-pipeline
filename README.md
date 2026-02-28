# Breweries Medallion Pipeline

This project implements a **Medallion Architecture data pipeline** using
**Apache Airflow** to ingest brewery data from a public API, apply
transformations and data quality validations, and generate analytical
datasets.

The pipeline is fully containerized using Docker and designed with
modularity, testability, and production-readiness principles in mind.

------------------------------------------------------------------------

## Architecture Overview

The project follows the **Medallion Architecture** pattern:

              +-----------+
              |   Bronze  |  Raw ingestion from API
              +-----------+
                     |
                     v
              +-----------+
              |   Silver  |  Cleaned & standardized data
              +-----------+
                     |
                     v
              +-----------+
              |    Gold   |  Aggregated analytical layer
              +-----------+

### Bronze

-   Raw data from API ingestion stored as JSON files. Partitioned by ingestion date using hierarchical directories: ingestion_year=YYYY/ingestion_month=MM/ingestion_day=DD. It is designed to preserve original payload structure and ensure reproducibility and traceability.

### Silver

-   Structured and standardized dataset stored in Parquet format. Data cleaning includes normalization, deduplication and type casting. Data was partitioned by country, ingestion_year, ingestion_month and ingestion_day. It offers efficient filtering by geography and time while preserving idempotent daily reprocessing.

### Gold

-   Aggregated dataset designed for analytical consumption. Stores country/state/type-level brewery counts. It was partitioned using the same hierarchical strategy as Silver (country, ingestion_year, ingestion_month and ingestion_day). It maintains consistent partitioning across Silver and Gold ensures structural symmetry and predictable downstream consumption patterns.

------------------------------------------------------------------------

## Technical Decisions & Architectural Considerations

### Storage Format — Why Parquet?

Parquet was chosen as the storage format due to its columnar nature, which provides efficient compression and optimized performance for analytical workloads. Since the pipeline is designed for downstream analytical consumption, a columnar format significantly reduces I/O and improves query efficiency compared to row-based formats such as CSV or JSON.

Additionally, Parquet integrates seamlessly with modern data processing engines such as Spark and query engines like Athena, making it a future-proof choice for scalable data lake architectures.

------------------------------------------------------------------------

### Data Partitioning — Why partition by `country` and ingestion date?

The dataset is partitioned by country followed by hierarchical ingestion date (year/month/day) to optimize analytical performance, storage organization, and controlled reprocessing.

This composite partitioning strategy provides several advantages:

-   Enables selective reads by country, reducing unnecessary data scanning
-   Isolates ingestion windows, allowing safe and idempotent reprocessing
-   Allows targeted reprocessing of a specific country without impacting other regions
-   Improves query performance for geographically scoped analyses
-   Maintains scalable storage organization as historical data grows
-   Prevents duplicate data accumulation through partition-level overwrite

Partitioning first by country supports common analytical patterns (e.g., country-level reporting) and enables partial backfills when data issues are isolated to a single region. Partitioning by ingestion date ensures temporal traceability, historical version control, and operational recovery in case of pipeline failures.

This approach aligns with modern data lake good practices, balancing performance, maintainability, and reprocessing flexibility in a production-oriented architecture.

------------------------------------------------------------------------

### Architecture Pattern — Why Medallion Architecture?

The Medallion Architecture (Bronze → Silver → Gold) was adopted to enforce a clear separation of responsibilities across data processing stages:

-   Bronze: Raw ingestion layer, preserving original data with minimal transformation
-   Silver: Cleaned and standardized data with validation rules applied
-   Gold: Business-ready, curated datasets optimized for analytics

This layered approach improves traceability, debugging, governance, and incremental evolution of the pipeline. It allows each transformation step to be independently validated and extended without impacting upstream data ingestion.

The architecture also ensures that raw data is always preserved, enabling reproducibility and reprocessing if business rules change over time.

------------------------------------------------------------------------

### Scalability & Cloud-Native Considerations

Although this project runs locally using Docker Compose for simplicity and reproducibility, it was designed with cloud scalability in mind.

In a production environment, Apache Airflow could be deployed on a Kubernetes cluster (e.g., Amazon EKS), allowing horizontal scaling of workers, better resource isolation, and improved fault tolerance.

For larger data volumes, the transformation layer could be migrated to a distributed processing engine such as Apache Spark running on managed services like AWS EMR or Databricks. This would enable efficient distributed computation, better memory management, and optimized execution for high-volume datasets.

In the current implementation, Pandas was intentionally selected due to the relatively small volume of data returned by the API. Introducing Spark at this stage would add unnecessary operational complexity. The decision prioritizes simplicity, readability, and faster development cycles while keeping the architecture extensible for future scaling.

------------------------------------------------------------------------

### Databricks & Delta Live Tables (DLT) Extension

If Databricks were adopted as the processing platform, the Medallion Architecture could be implemented using Delta Live Tables (DLT).

DLT enables declarative pipeline definitions with built-in data quality enforcement, automatic dependency management, and native orchestration within the Databricks environment. Each Medallion layer could be expressed as managed Delta tables with defined expectations (e.g., schema validation, null constraints, and business rules).

This approach would provide:

-   Native incremental processing
-   Built-in data quality monitoring
-   Automatic lineage tracking
-   Simplified operational maintenance
-   Optimized storage via Delta Lake

In such a scenario, Airflow could remain as a high-level orchestrator triggering Databricks workflows, or be replaced entirely by Databricks Workflows depending on governance and platform standards.

This demonstrates that the current implementation is intentionally lightweight but architecturally aligned with modern Lakehouse principles, allowing seamless evolution into a fully managed, production-grade data platform.

------------------------------------------------------------------------

### Testing Strategy

-   We use Unit Tests for transformations and data quality testing
-   Isolation of domain logic from orchestration layer

Run tests with:

``` bash
pytest
```

------------------------------------------------------------------------

### Data Quality Strategy

Validation appled on Silver layer using the following:

-   Dataset cant be empty
-   No duplicate IDs are allowed
-   Maximum percentage of null latitude (it may be configured using Airflow variables)
-   Maximum percentage of unknown countries (it may be configured using Airflow variables)

Data Quality metrics are pushed via XCom for potential monitoring extensions.

------------------------------------------------------------------------

## How to Run the Project

### Clone the repository

``` bash
git clone https://github.com/romuloafm-r91/breweries-medallion-pipeline.git
cd breweries-medallion-pipeline
```

------------------------------------------------------------------------

### Create `.env` file

Copy the example file in the project root:

``` bash
cp .env.example .env
```

Update the required values, including your **Google App Password** for email notifications. 
If you want to know more aboutGoogle App Password, please access https://support.google.com/accounts/answer/185833

------------------------------------------------------------------------

### Build and start containers

``` bash
docker compose up -d --build
```

------------------------------------------------------------------------

### Set folder permissions

Adjust directory permissions to ensure the Airflow container can properly read DAGs and write processed data:

``` bash
sudo chown -R 50000:0 dags src
sudo chmod -R 755 dags src
sudo chown -R 50000:0 data-lake
sudo chmod -R 775 data-lake

```

-   755 > Read and execute access for DAGs and source code
-   775 > Write permission required for the data lake output directory

Note: sudo may be required depending on how Docker created the mounted volumes.

------------------------------------------------------------------------

### Access Airflow

Airflow UI:

    http://localhost:8080

The Default Password and User are 'admin'

### Import Airflow Variables

-   Go to **Admin → Variables**
-   Import the file:

    airflow_variables.json

This configures the Data Quality thresholds.

Return to DAGs option and enable the DAG:

    brewery_medallion_pipeline

------------------------------------------------------------------------

## Pipeline Flow

1.  Extract data from public Brewery API
2.  Save raw data in Bronze layer
3.  Transform into structured Silver dataset
4.  Apply Data Quality validations
5.  Generate aggregated Gold dataset

Task dependency:

    extract → silver → data_quality → gold

------------------------------------------------------------------------

## Observability

-   Logging via Python logging module
-   Email notifications on failure
-   Data Quality metrics pushed via XCom

------------------------------------------------------------------------

## Author

Romulo Miranda