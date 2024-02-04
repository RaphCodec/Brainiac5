import pandas as pd
import pyodbc
from tqdm import tqdm

def Connect(Server,Database,Driver):
    conn = pyodbc.connect(
        f'''
        Driver={Driver};
        Server={Server};
        Database={Database};
        Trusted_Connection=yes;
        '''
        )
    cursor = conn.cursor()
    return cursor, conn 

def CreateTable(df,
                table: str,
                primary: str | list = None,
                primaryName: str = None,
                foreign: str | list = None,
                foreignName: str = None,
                foreignTable: str = None,
                foreignRelated: str | list = None,
                unique: str | list = None,
                uniqueName: str = None,
                charbuff: int = 10,
                saveQuery:bool = False,
                savePath:str = None
                ) -> str:

    if not isinstance(table, str):
        raise ValueError('Table argument must be str')

    if foreign and not foreignTable:
        raise TypeError('Foreign Table must be supplied if a foreign key is to be added')

    # Define a mapping of pandas data types to SQL Server data types
    sql_data_types = {
        'int64': 'INT',
        'float64': 'FLOAT',
        'object': 'VARCHAR',
        'datetime64[ns]': 'DATETIME',
        'bool': 'BIT',
    }

    # Get the columns and their data types from the DataFrame
    columns = df.columns
    dtypes = df.dtypes

    # Create the CREATE TABLE statement
    create_table_query = f"CREATE TABLE [{table}] (\n"

    for column, dtype in zip(columns, dtypes):
        sql_type = sql_data_types.get(str(dtype), 'VARCHAR(max)')  # defaulting to VARCHAR(max) if no datatype is matched
        if dtype == 'object':
            max_length = df[column].astype(str).apply(len).max() + charbuff
            create_table_query += f"    [{column}] {sql_type}({max_length}),\n"
        else:
            create_table_query += f"    [{column}] {sql_type},\n"
        if (primary != None and column in primary) or column == primary: #ensures that primary key columns are not null
            create_table_query = create_table_query[:-2]
            create_table_query += " NOT NULL,\n"

    create_table_query = create_table_query.rstrip(',\n') + "\n);"

    # Adding primary key(s) if needed
    if primary is not None:
        primary_key_constraint = f'CONSTRAINT [{primaryName}] PRIMARY KEY ' if primaryName else 'PRIMARY KEY '
        primary_keys = primary if isinstance(primary, str) else ", ".join([f'[{key}]' for key in primary])
        create_table_query += f'\nALTER TABLE [{table}]\nADD {primary_key_constraint}({primary_keys});\n'

    # Adding foreign key(s) if needed
    if foreign is not None and foreignName is not None and foreignTable is not None and foreignRelated is not None:
        foreign_key_constraint = f'CONSTRAINT [{foreignName}] FOREIGN KEY ' if foreignName else 'FOREIGN KEY '
        foreign_keys = foreign if isinstance(foreign, str) else ", ".join([f'[{key}]' for key in foreign])
        foreign_related_keys = foreignRelated if isinstance(foreignRelated, str) else ", ".join([f'[{key}]' for key in foreignRelated])
        create_table_query += f'\nALTER TABLE [{table}]\nADD {foreign_key_constraint}({foreign_keys})\nREFERENCES [{foreignTable}] ({foreign_related_keys});\n'

    # Adding unique key(s) if needed
    if unique is not None:
        unique_constraint = f'CONSTRAINT [{uniqueName}] UNIQUE ' if uniqueName else 'UNIQUE '
        unique_keys = unique if isinstance(unique, str) else ", ".join([f'[{key}]' for key in unique])
        create_table_query += f'\nALTER TABLE [{table}]\nADD {unique_constraint}({unique_keys});\n'
        
    if saveQuery == True:
        path = f'CreateTable - {table}.sql'
        if savePath:
            path = savePath + f'\CreateTable - {table}.sql'
        with open(path, 'w') as sql_file:
            sql_file.write(create_table_query)

    return create_table_query

def insert(columns: list, table: str, saveQuery:bool = False, savePath:str = None) -> str:
    if not isinstance(columns, list):
        raise ValueError('Columns value must be a list')

    if not isinstance(table, str):
        raise ValueError('Table value must be a str')

    columns_str = ',\n'.join([f'[{col}]' for col in columns])
        
    query = f'''
    INSERT INTO [{table}]
    ({columns_str})
    Values({('?,' * len(columns))[:-1]});
    '''
    
    if saveQuery == True:
        path = f'InsertQuery - {table}.sql'
        if savePath:
            path = savePath + f'\InsertQuery - {table}.sql'
        with open(path, 'w') as sql_file:
            sql_file.write(query)
    
    return query

def update(columns: list, table: str, where: str | list, saveQuery:bool = False, savePath:str = None) -> str:
    if not isinstance(columns, list):
        raise ValueError('Columns value must be a list')

    if not isinstance(table, str):
        raise ValueError('Table value must be a str')

    if not isinstance(where, (list, str)):
        raise ValueError('Where value must be a list or a string')

    columns = [f'[{value}] = ?\n' for value in columns if value not in where]

    if isinstance(where, list):
        where_clause = ' AND '.join([f'[{value}] = ?' for value in where])
    else:
        where_clause = f'[{where}] = ?'

    query = f'''
    UPDATE [{table}]
    SET 
    {','.join(columns)}
    WHERE {where_clause};
    '''
    
    if saveQuery == True:
        path = f'UpdateQuery - {table}.sql'
        if savePath:
            path = savePath + path
        with open(path, 'w') as sql_file:
            sql_file.write(query)

    return query

def RunQuery(df,query:str,cursor,conn, batchSize:int = 20_000, NoChunking:bool = False):
    df  = df.astype(object).where(pd.notnull(df), None)
    
    cursor.fast_executemany = True
    
    if NoChunking:
        cursor.executemany(query, df.values.tolist())
        conn.commit()
        return
    
    # Get the total number of rows for the progress bar
    total_rows = len(df)
    
    # Use tqdm to create a progress bar
    with tqdm(total=total_rows, desc="Processing rows", unit="row") as pbar:
        rows_list = df.values.tolist()
        #splitting df into batches and executing query
        for i in range(0, len(rows_list), batchSize):
            batch = rows_list[i:i + batchSize]
            
            # Execute the query for the current batch
            cursor.executemany(query, batch)
            conn.commit()
            
            # Update the progress bar
            pbar.update(len(batch))

    # Close the progress bar after completion
    pbar.close()
    return
    