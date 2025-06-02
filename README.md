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

## Deploying with DBX

This project is configured for deployment using [Databricks DBX](https://docs.databricks.com/dev-tools/dbx.html).

### 1. Install DBX

```bash
pip install dbx
```


### 3. Deploy Job

```bash
dbx deploy --jobs turbine-job --environment dev
```

### 4. Launch the Job

```bash
dbx launch turbine-job --environment dev --parameters='{"input_path": "value1"}'
```

This will run `src/main.py` on your specified cluster with the input path:

```
dbfs:/FileStore/ashok/*.csv
```