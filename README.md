# Turbine Data Processing Pipeline

This project contains a scalable and testable PySpark pipeline for processing turbine sensor data in Databricks. It includes data cleaning, summary statistics calculation, anomaly detection, and saving results to Unity Catalog-managed Delta tables.



## Running Unit Tests

Make sure you have a working Python environment and install dependencies:

```bash
pip install -r requirements.txt
```

Then run tests using:

```bash
pytest tests/
```

---

## Databricks Asset bundles

This project is configured for deployment using Databricks Asset bundles

### 1. Initialize a bundle using the default Python bundle project template.

```bash
databricks bundle init
```


### 3. Validate the project's bundle

```bash
databricks bundle validate
```

### 4. Deploy the Job

```bash
databricks bundle deploy -t dev
```

### 4. Run the deployed project

```bash
databricks bundle run -t dev turbine_job
This will run `src/main.py` on your specified cluster with the input path:
```

```
dbfs:/FileStore/ashok/*.csv
```

```
The outputs are stored in output/
```