from datetime import  timedelta

from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from util_functions import (
    ingest_data_to_duckdb,
    filter_permits_for_last_n_years,
    data_stats,
    find_column_pair_with_highest_duplications,
    deduplicate_data,
    aggregate_data,
    rename_columns,
    save_as_sqlite,
    DB_PATH,
    SQLITE_DB_PATH,
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 1),
    'retries': 1,
}

# Define the DAG
with DAG(
    'construction_permits_processing',
    default_args=default_args,
    description='Process construction permits data',
    schedule_interval=None, 
    catchup=False
) as dag:

    dag.doc_md = """
    # Construction Permits Processing DAG" \
    "" \
    This DAG processes construction permits data from a CSV file, filters it for the last 5 years, \
    generates statistics, finds duplicates, deduplicates the data, aggregates it, renames columns, and saves it to a SQLite database.

    ## Input data products
    - Construction_Permits.csv: The input CSV file containing construction permits data.
    ## Output data products
    - filtered_permits_deduplicated: The deduplicated permits data.
    - filtered_permits_deduplicated_agg: The aggregated permits data.

    """


    @task
    def ingest_data():
        ingest_data_to_duckdb(
            input_file="./Construction_Permits.csv",
            table_name="permits",
            db_path=DB_PATH
        )

    @task
    def filter_permits():
        filter_permits_for_last_n_years(
            table_name="permits",
            years=5,
            db_path=DB_PATH
        )

    @task
    def generate_stats():
        data_stats(
            table_name="filtered_permits",
            db_path=DB_PATH
        )

    @task
    def find_highest_duplicates():
        return find_column_pair_with_highest_duplications(
            columns=[
                "Description",
                "OriginalAddress",
                "COBPIN",
                "BOCOPIN",
                "BOCOTAX",
                "ProjectName",
                "Description",
            ],
            table_name="filtered_permits",
            db_path=DB_PATH
        )

    @task
    def deduplicate():
        deduplicate_data(
            table_name="filtered_permits",
            columns=("IssuedDate", "ProjectName", "Description", "OriginalAddress"),
            db_path=DB_PATH
        )

    @task
    def aggregate():
        aggregate_data(
            table_name="filtered_permits_deduplicated",
            db_path=DB_PATH
        )

    @task
    def rename():
        rename_columns(
            table_name="filtered_permits_deduplicated",
            column_names={
                "COBPIN": "UniqueParcelIdentificationNumber",
                "BOCOPIN": "AssessorParcelIdentificationNumber",
                "BOCOTAX": "AssessorTaxAccountNumber",
            },
            db_path=DB_PATH
        )

    @task
    def save():
        save_as_sqlite(
            table_names=["filtered_permits_deduplicated", "filtered_permits_deduplicated_agg"],
            sqlite_path=SQLITE_DB_PATH,
            duckdb_path=DB_PATH
        )

    ingest_task = ingest_data()
    filter_task = filter_permits()
    stats_task = generate_stats()
    dupes_task = find_highest_duplicates()
    deduplication_task = deduplicate()
    agg_task = aggregate()
    rename_task = rename()
    save_task = save()

    ingest_task >> filter >> [stats_task, dupes_task] >> deduplication_task >> agg_task >> rename_task >> save_task
