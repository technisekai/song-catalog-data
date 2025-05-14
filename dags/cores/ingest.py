import pandas as pd

def ingestion_into_clickhouse(conn, table_name: str, data: list[dict]) -> None:
    # Create table if doesnt exists
    q_create_table = f"""
    create table if not exists {table_name} (
        {" ".join([f"{x.replace(' ', '_').lower().strip()} String," for x in data[0].keys()])} _created_at DateTime DEFAULT now()
    ) engine = MergeTree order by _created_at
    """
    conn.command(q_create_table)
    print(f"INF {q_create_table}")
    # Insert data
    conn.insert(
        table_name,
        pd.DataFrame(data),
        column_names=data[0].keys()
    )