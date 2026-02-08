import pandas as pd

def load_main_log(conn, cursor, df):
    """
    Load transformed runs into main_log table.
    """
    insert_columns = df.columns.tolist()
    placeholders = ",".join(["?"] * len(insert_columns))
    col_names = ",".join(insert_columns)

    sql = f"""
    INSERT OR IGNORE INTO main_log ({col_names})
    VALUES ({placeholders});
    """

    data = df.values.tolist()

    cursor.executemany(sql, data)
    conn.commit()

    print(f"Load Garmin main_log is done. Inserted {cursor.rowcount} rows into main_log")
