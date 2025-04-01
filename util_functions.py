
DB_PATH = "./file.db"
SQLITE_DB_PATH = "./db.sqlite"


def ingest_data_to_duckdb(input_file: str, table_name: str, db_path: str = DB_PATH):
    """
    Ingests data from a CSV file into a DuckDB database.
    This function creates a table in the DuckDB database and loads data from a CSV file into it.
    """

    import duckdb
    con = duckdb.connect(db_path)

    # Ingest data from CSV into the DuckDB table
    con.execute(
        f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv('{input_file}');"
    )

    # Close the connection
    con.close()


def data_stats(table_name: str, db_path: str = DB_PATH):
    """
    Computes and returns basic statistics from the data in the DuckDB database.
    This function connects to the DuckDB database, executes a query to get the count of records,
    and returns the result.
    """
    import duckdb

    con = duckdb.connect(db_path)

    # Execute a query to get basic statistics
    result = con.execute(
        f"""        SELECT
    -- Count distinct values for key columns
    COUNT(DISTINCT PermitID) AS distinct_permitid,
    COUNT(DISTINCT PermitNum) AS distinct_permitnum,
    COUNT(DISTINCT MasterPermitNum) AS distinct_masterpermitnum,
    COUNT(DISTINCT OriginalAddress) AS distinct_address,
    COUNT(DISTINCT AppliedDate) AS distinct_applieddate,
    
    -- Total rows
    COUNT(*) AS total_rows,
    
    -- Percentage of NULLs
    SUM(CASE WHEN PermitID IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS permitid_null_pct,
    SUM(CASE WHEN PermitNum IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS permitnum_null_pct,
    SUM(CASE WHEN MasterPermitNum IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS masterpermitnum_null_pct,
    SUM(CASE WHEN OriginalAddress IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS address_null_pct,
    SUM(CASE WHEN AppliedDate IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS applieddate_null_pct,
    
    -- Duplication rate (rows with duplicate values in a column)
    (COUNT(*) - COUNT(DISTINCT PermitID)) * 100.0 / COUNT(*) AS permitid_dup_pct,
    (COUNT(*) - COUNT(DISTINCT PermitNum)) * 100.0 / COUNT(*) AS permitnum_dup_pct,
    (COUNT(*) - COUNT(DISTINCT MasterPermitNum)) * 100.0 / COUNT(*) AS masterpermitnum_dup_pct,
    (COUNT(*) - COUNT(DISTINCT OriginalAddress)) * 100.0 / COUNT(*) AS address_dup_pct,
    (COUNT(*) - COUNT(DISTINCT AppliedDate)) * 100.0 / COUNT(*) AS applieddate_dup_pct
FROM {table_name}; 

        """
    ).df()

    print(f"Data Statistics:{table_name}, {result.head()}")

    con.close()


def find_column_pair_with_highest_duplications(
    columns: list[str], table_name: str, db_path: str = DB_PATH
):
    """
    Finds the pair of columns with the highest duplication percentage in the DuckDB database.
    This function connects to the DuckDB database, executes a query to find the duplication percentage,
    and returns the pair of columns with the highest duplication percentage.
    """
    import duckdb
    from itertools import combinations

    con = duckdb.connect(db_path)

    results = []

    # Loop over all pairs of columns
    for col1, col2 in combinations(columns, 2):
        query = f"""
        SELECT
            COUNT(*) - COUNT(DISTINCT ({col1}, {col2})) AS duplicate_count,
            (COUNT(*) - COUNT(DISTINCT ({col1}, {col2}))) * 100.0 / COUNT(*) AS duplicate_percent 
        FROM {table_name}
        """
        duplicate_count = con.sql(query).fetchone()[0]
        duplicate_percent = con.sql(query).fetchone()[1]

        # Store the result
        results.append(
            {
                "pair": (col1, col2),
                "duplicate_count": duplicate_count,
                "duplicate_percentage": duplicate_percent,
            }
        )

    highest_dup_pair = max(results, key=lambda x: x["duplicate_percentage"])

    print("Duplication percentages for column pairs:")
    for result in sorted(
        results, key=lambda x: x["duplicate_percentage"], reverse=True
    ):
        print(
            f"Pair: {result['pair']}, Duplicate Count: {result['duplicate_count']}, "
            f"Duplicate %: {result['duplicate_percentage']:.2f}%"
        )

    print("\nPair with highest duplication percentage:")
    print("-----------------------------------------")
    print(
        f"Pair: {highest_dup_pair['pair']}, Duplicate Count: {highest_dup_pair['duplicate_count']}, "
        f"Duplicate %: {highest_dup_pair['duplicate_percentage']:.2f}%"
    )

    # Close the connection
    con.close()
    return highest_dup_pair["pair"]


def filter_permits_for_last_n_years(
    table_name: str, years: int, db_path: str = DB_PATH
):
    """
    Filters the permits in the DuckDB database for the last 'n' years based on the AppliedDate.
    This function connects to the DuckDB database, executes a query to filter the data,
    and returns the filtered results.
    """
    import duckdb

    con = duckdb.connect(db_path)

    result = con.execute(
        f"""
        CREATE OR REPLACE TABLE filtered_{table_name} AS
        SELECT *
        FROM permits
        WHERE IssuedDate >=  CURRENT_DATE - INTERVAL '{years} years';
        """
    )
    print(result)
    con.close()


def aggregate_data(table_name: str, db_path: str = DB_PATH):
    import duckdb

    con = duckdb.connect(db_path)
    con.sql(
        f"""
    CREATE OR REPLACE TABLE {table_name}_agg AS
    SELECT
        EXTRACT(YEAR FROM IssuedDate) AS issue_year,
        PermitType,
        COUNT(*) AS permit_count
    FROM {table_name}
    GROUP BY 
        EXTRACT(YEAR FROM IssuedDate),
        PermitType
    """
    )

    # Close connection
    con.close()


def save_as_sqlite(
    table_names: list[str], sqlite_path: str, duckdb_path: str = DB_PATH
):
    """
    Save the DuckDB database as a SQLite database.
    This function connects to the DuckDB database, exports the data to a SQLite database file.
    """
    import duckdb
    import sqlite3

    duckdb_con = duckdb.connect(duckdb_path)
    sqlite_conn = sqlite3.connect(sqlite_path)

    for table_name in table_names:
        df = duckdb_con.execute(f"SELECT * FROM {table_name}").df()

        df.to_sql(table_name, sqlite_conn, if_exists="replace", index=False)

    sqlite_conn.close()
    duckdb_con.close()


def deduplicate_data(table_name, columns, db_path: str = DB_PATH):
    """
    Deduplicate data in the DuckDB database.
    This function connects to the DuckDB database and removes duplicate rows based on specified columns.
    """
    import duckdb

    con = duckdb.connect(db_path)
    con.sql(
        f"""CREATE OR REPLACE TABLE {table_name}_deduplicated AS
            WITH deduped AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY {",".join(columns)}
                        ORDER BY ObjectId 
                    ) AS rn
                FROM {table_name} 
            )
            SELECT 
                *
            FROM deduped
            WHERE rn = 1;"""
    )

    # Close connection
    con.close()


def rename_columns(table_name, column_names, db_path: str = DB_PATH):
    """
    Rename columns in the DuckDB database.
    This function connects to the DuckDB database and renames columns in a specified table.
    """
    import duckdb

    con = duckdb.connect(db_path)

    for key, value in column_names.items():
        # Use the ALTER TABLE command to rename columns
        con.execute(
            f"""
            ALTER TABLE {table_name}
            RENAME COLUMN {key} TO {value};
            """
        )

    con.close()