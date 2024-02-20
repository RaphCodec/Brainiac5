import pandas as pd
import pyodbc
from tqdm import tqdm
from colorama import Fore, Style

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

def MakeInsertQuery(columns: list, table: str, saveQuery:bool = False, savePath:str = None) -> str:
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

def MakeUpdateQuery(columns: list, table: str, where: str | list, saveQuery:bool = False, savePath:str = None) -> str:
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

def RunQuery(df,query:str,conn, ChunkSize:int = 20_000, NoChunking:bool = False, BarDesc:str = 'Processing rows') -> None:
    df  = df.astype(object).where(pd.notnull(df), None)
    
    cursor = conn.cursor()
    
    cursor.fast_executemany = True
    
    errors = pd.DataFrame()
    
    if NoChunking:
        try:
            cursor.executemany(query, df.values.tolist())
            conn.commit()
            print(Fore.GREEN + f'\nQuery Executed Successfully.' + Style.RESET_ALL)
        except Exception as e:
            print(Fore.RED + f'\nError: {e}' + Style.RESET_ALL)
        return
    
    # Get the total number of rows for the progress bar
    total_rows = len(df)
    rows_processed = 0
    
    if ChunkSize > len(df):
        ChunkSize = len(df)
    
    batch_num = 1
    
    # Use tqdm to create a progress bar
    
    for i in range(0, len(df), ChunkSize):
        batch = df.iloc[i:i + ChunkSize]  # Get the current batch of rows
        with tqdm(total=len(batch), desc=BarDesc, unit="row", colour="blue", ascii=' =') as pbar:
            try:
                # Execute the query for the current batch
                cursor.executemany(query, batch.values.tolist())
                conn.commit()
                
                # Update the progress bar for each iteration
                pbar.update(len(batch))
                
                # Update the progress bar description dynamically
                processed_rows = min(pbar.n, total_rows)
                
                rows_processed += processed_rows
                
                batch_num += 1
                
            except Exception as e:
                print(Fore.RED + f'\nError in Batch {batch_num}' + Style.RESET_ALL)
                errors = pd.concat([errors, batch])  # Append error rows to the DataFrame
                batch_num += 1
        # Close the progress bar after completion or error
        pbar.close()
    
    print(Fore.GREEN + f'Sucessfully processed {rows_processed}/{total_rows}.' + Style.RESET_ALL)
       
    if not errors.empty:
        cursor.fast_executemany = False
        print(Fore.YELLOW + 'Attempting to find errors' + Style.RESET_ALL)  
        errors['Error'] = None
        for idx, row in errors.iloc[:, :-1].iterrows():
            try:
                cursor.execute(query, row.tolist())
                rows_processed += 1
                print('here')
            except Exception as e:
                errors.at[idx, 'Error'] = e
                print('error')
        # conn.commit()
        
        errors.to_csv('Errors.csv')
        
        print(Fore.GREEN + f'New Total after finding errors: {rows_processed}/{total_rows} rows Successful.' + Style.RESET_ALL
            ,Fore.RED + f'\n{total_rows - rows_processed} rows with errors.' + Style.RESET_ALL)
            
        
    
    return
    