from util_functions import *


ingest_data_to_duckdb(
    input_file="./Construction_Permits.csv",
    table_name="permits",
    db_path=DB_PATH,
)

filter_permits_for_last_n_years(
    table_name="permits",
    years=5,
    db_path=DB_PATH,
)

data_stats(
    table_name="filtered_permits",
    db_path=DB_PATH,
)

highest_dup_pair = find_column_pair_with_highest_duplications(
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
    db_path=DB_PATH,
)

deduplicate_data(
    table_name="filtered_permits",
    columns=("IssuedDate", "ProjectName", "Description", "OriginalAddress"),
    db_path=DB_PATH,
)

aggregate_data(
    table_name="filtered_permits_deduplicated",
    db_path=DB_PATH,
)

rename_columns(
    table_name="filtered_permits_deduplicated",
    column_names={
        "COBPIN": "UniqueParcelIdentificationNumber",
        "BOCOPIN": "AssessorParcelIdentificationNumber",
        "BOCOTAX": "AssessorTaxAccountNumber",
    },
    db_path=DB_PATH,
)
save_as_sqlite(
    table_names=["filtered_permits_deduplicated", "filtered_permits_deduplicated_agg"],
    sqlite_path=SQLITE_DB_PATH,

    duckdb_path=DB_PATH,
)
