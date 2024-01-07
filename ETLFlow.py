import pandas as pd
import numpy as np
import pyodbc

# Create dummy data
data = {
    'Name': ['Alice', 'Bob', 'Charlie'],
    'Age': [25, 30, 22],
    'Height': [5.6, 6.0, 5.4],
    'IsStudent': [True, False, True],
    'Scores': [85.5, 90.0, 78.5],
    'BirthDate': pd.to_datetime(['1995-03-15', '1990-08-22', '2000-05-10']),
    'RegistrationDateTime': pd.to_datetime(['2022-01-01 08:30', '2022-01-02 10:45', '2022-01-03 12:15']),
    'MeetingTime': pd.to_datetime(['12:30', '14:00', '11:45']).time,
    'Weight': [62.5, 75.0, 58.3]
}

# Create Pandas DataFrame with specified data types for each column
df = pd.DataFrame(data)

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
                uniqueName: str = None
                ) -> str:

    if not isinstance(table, str):
        raise ValueError('Table argument must be str')

    if foreign and not foreignTable:
        raise TypeError('Foreign Table must be supplied if a foreign key is to be added')

    # Define a mapping of pandas data types to SQL Server data types
    sql_data_types = {
        'int64': 'INT',
        'float64': 'FLOAT',
        'object': 'VARCHAR(max)',
        'datetime64[ns]': 'DATETIME',
        'bool': 'BIT',
    }

    # Get the columns and their data types from the DataFrame
    columns = df.columns
    dtypes = df.dtypes

    # Create the CREATE TABLE statement
    create_table_query = f"CREATE TABLE {table} (\n"

    for column, dtype in zip(columns, dtypes):
        sql_type = sql_data_types.get(str(dtype), 'VARCHAR(max)') #defaulting to VARCHAR(max) if no datatype is matched
        create_table_query += f"    {column} {sql_type},\n"

    create_table_query = create_table_query.rstrip(',\n') + "\n);"

    # Adding primary key(s) if needed
    if primary is not None:
        primary_key_constraint = f'CONSTRAINT {primaryName} PRIMARY KEY ' if primaryName else 'PRIMARY KEY '
        primary_keys = primary if isinstance(primary, str) else ", ".join(primary)
        create_table_query += f'ALTER TABLE {table}\nADD {primary_key_constraint}({primary_keys});\n'

    # Adding foreign key(s) if needed
    if foreign is not None and foreignName is not None and foreignTable is not None and foreignRelated is not None:
        foreign_key_constraint = f'CONSTRAINT {foreignName} FOREIGN KEY ' if foreignName else 'FOREIGN KEY '
        foreign_keys = foreign if isinstance(foreign, str) else ", ".join(foreign)
        foreign_related_keys = foreignRelated if isinstance(foreignRelated, str) else ", ".join(foreignRelated)
        create_table_query += f'ALTER TABLE {table}\nADD {foreign_key_constraint}({foreign_keys})\nREFERENCES {foreignTable} ({foreign_related_keys});\n'

    # Adding unique key(s) if needed
    if unique is not None:
        unique_constraint = f'CONSTRAINT {uniqueName} UNIQUE ' if uniqueName else 'UNIQUE '
        unique_keys = unique if isinstance(unique, str) else ", ".join(unique)
        create_table_query += f'ALTER TABLE {table}\nADD {unique_constraint}({unique_keys});\n'

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
    
query = CreateTable(df,'Table')
print(query)