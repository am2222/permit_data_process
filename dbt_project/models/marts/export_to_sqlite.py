def model(dbt, session):
    deduplicated_permits = dbt.ref('deduplicated_permits')
    renamed_permits = dbt.ref('renamed_permits')

    import sqlite3
    import duckdb

    sqlite_conn = sqlite3.connect('./db.sqlite')

    duckdb_con = duckdb.connect('./file.db')

    for table_name in ['deduplicated_permits', 'renamed_permits']:
        df = duckdb_con.execute(f"SELECT * FROM {table_name}").df()
        df.to_sql(table_name, sqlite_conn, if_exists="replace", index=False)

    df_to_return = duckdb_con.execute(f"SELECT * FROM renamed_permits").df()
    
    sqlite_conn.close()
    duckdb_con.close()
    
    return df_to_return