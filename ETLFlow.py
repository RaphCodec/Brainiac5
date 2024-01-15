import pandas as pd
import numpy as np
import pyodbc
import time

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
                charbuff: int = 10
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
        if column in primary or column == primary: #ensures that primary key columans are not null
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

    return create_table_query

def insert(columns:list, table:str):
    if not isinstance(columns, list):
        raise ValueError ('Columns value must be a list')
    
    if not isinstance(table, str):
        raise ValueError ('Table value must be a str')
    
    return f'''
    INSERT INTO {table}
    {', '.join(columns)}
    Values({('?,' * len(columns))[:-1]})
    '''

def update(columns:list, table:str, where:str|list):
    if not isinstance(columns, list):
        raise ValueError ('Columns value must be a list')
    
    if not isinstance(table, str):
        raise ValueError ('Table value must be a str')
    
    if not isinstance(columns, list):
        raise ValueError ('Where value must be a list')
    
    columns = [
                value for value in columns
                if value not in where
               ]
    
    if isinstance(where,list):
        where = ' and, '.join([value + ' = ?' for value in where])
    else:
        where += ' = ?'
    
    query = f'''
    UPDATE {table}
        SET {', '.join(
            [
            column + ' = ?'
            for column in columns
                ])
                }
        WHERE {where}
    '''
    return query
    