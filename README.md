
# Processing Sample Permit Data with DuckDB and dbt

This project demonstrates how to process a CSV file containing permit data using DuckDB as an in-memory processing engine, with the results exported to a SQLite database. All operations are performed using SQL to avoid memory limitations encountered with libraries like Pandas or Polars. Three distinct approaches are showcased:

- A standalone Python script
- An Airflow DAG
- A dbt workflow

## Project Structure

- **`dbt_project/`**: Contains the dbt-based implementation
- **`construction_permits_data_dictionary`**: Data dictionary file
- **`Construction_Permits`**: Input CSV file
  - **Note**: Please copy a version of `construction_permits.csv` into this folder. The existing version includes data from 2000 to the present only, intentionally reduced in size to allow uploading to Git (no access to Git LFS).
- **`dag.py`**: Airflow DAG definition
- **`etl_pipeline.py`**: Standalone Python script
- **`requirements.txt`**: Python package dependencies
- **`util_function.py`**: Shared utility functions to reduce code duplication in `etl_pipeline.py` and `dag.py`

## Setup

1. Create and activate a Python virtual environment.
2. Install the required packages with the following command:

   ```sh
   pip install -r requirements.txt
   ```

## Running the Standalone Python Script

Execute the script using:

```sh
python etl_pipeline.py
```

### Output
- The script displays data statistics in the console.
- It generates a `db.sqlite` file containing two tables:
  - **`filtered_permits_deduplicated`**: Deduplicated permit data based on the columns `"IssuedDate"`, `"ProjectName"`, `"Description"`, and `"OriginalAddress"`. Column names are standardized, with select columns renamed to `camelCase`.
  - **`filtered_permits_deduplicated_agg`**: Aggregated data derived from the deduplicated table.

## Running the dbt Workflow

The dbt implementation is located in the `dbt_project/` directory. Navigate to this folder and run:

```sh
dbt seed
dbt run
```

### Output
- A `db.sqlite` file is created with two tables:
  - **`filtered_permits_deduplicated`**: Deduplicated permit data using the columns `"IssuedDate"`, `"ProjectName"`, `"Description"`, and `"OriginalAddress"`. Column names are standardized, with some renamed to `camelCase`.
  - **`filtered_permits_deduplicated_agg`**: Aggregated data based on the deduplicated table.

## Running the Airflow DAG

A sample Airflow DAG is provided in `dag.py`. It leverages the same utility functions as the standalone script, with imports defined at the function level to comply with Airflow's DAG-level import restrictions.

## Potential Improvements

- Organize the Airflow DAG using task groups for better readability and maintainability.
- Enhance deduplication logic by conducting a deeper analysis of the data.
- Refactor `util_function.py` into a more object-oriented, class-based structure for improved modularity.

