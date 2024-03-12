import pandas as pd
import pyodbc
from brainiac5 import CreateTable, MakeInsertQuery, MakeUpdateQuery, RunQuery

# Function to establish connection to SQL Server
def connect_to_sql_server(server, database):
    conn_str = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};Trusted_connection=YES;"
    conn = pyodbc.connect(conn_str)
    return conn

# Sample DataFrame
data = {
    'ID': [1, 2, 3],
    'Name': ['Alice', 'Bob', 'Charlie'],
    'Age': [30, 25, 35]
}
df = pd.DataFrame(data)

# SQL Server connection parameters
server = 'SERVER'
database = 'DB'

# Establish connection
conn = connect_to_sql_server(server, database)

# Test CreateTable function
create_table_query = CreateTable(df, 'TestTable', primary=['ID'])

print(create_table_query)

cursor = conn.cursor()

# cursor.execute(create_table_query)
# conn.commit()


# Test MakeInsertQuery function
insert_query = MakeInsertQuery(df.columns.tolist(), 'TestTable')

print(insert_query)

# Test RunQuery function (inserting data)
RunQuery(df, insert_query, conn)

# Sample data for update. Columns must be in the same order in the dataframe as they appear in the query
update_data = {
    'Name': ['Alice', 'John'],
    'Age': [31, 26],
    'ID':[1, 2]
}
update_df = pd.DataFrame(update_data)

# Test MakeUpdateQuery function
update_query = MakeUpdateQuery(update_df.columns.tolist(), 'TestTable', where=['ID'])

# Test RunQuery function (updating data)
RunQuery(update_df, update_query, conn, NoChunking=True)

# Close connection
conn.close()
