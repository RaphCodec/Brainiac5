import pandas as pd
import pyodbc
from tqdm import tqdm
from colorama import Fore, Style
import warnings

#this fucntion returns the maximum number of digits both before and after the decimal point across all column values
def DecimalCount(value):
    value_str = str(value)
    if '.' in value_str:
        before_decimal = len(value_str.split('.')[0])
        after_decimal = len(value_str.split('.')[1])
    else:
        before_decimal = len(value_str)
        after_decimal = 0
    return pd.Series({'Max_Before': before_decimal, 'Max_After': after_decimal})

def IntType(column):
    """
    Checking for integer data types and their respective ranges:
    INT: 4-byte integer (-2,147,483,648 to 2,147,483,647)
    BIGINT: 8-byte integer (-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807)
    SMALLINT: 2-byte integer (-32,768 to 32,767)
    TINYINT: 1-byte integer (0 to 255)
    """

    min_val = column.min()
    max_val = column.max()
    
    if min_val in range(0, 256) and max_val in range(0, 256):
        return 'TINYINT'
    elif (min_val in range(0, 256) or max_val in range(0, 256)) and (min_val not in range(0, 256) or max_val not in range(0, 256)):
        warnings.warn(Fore.YELLOW + f'\nWARNING: Potential outlier in column: {column.name}. Max Number: {max_val}. Min Number: {min_val}' + Style.RESET_ALL, stacklevel=2)
        return 'SMALLINT'
    elif min_val in range(-32_768, 32_768) and max_val in range(-32_768, 32_768):
        return 'SMALLINT'
    elif (min_val in range(-32_768, 32_768) or max_val in range(-32_768, 32_768)) and (min_val not in range(-32_768, 32_768) or max_val not in range(-32_768, 32_768)):
        warnings.warn(Fore.YELLOW + f'\nWARNING: Potential outlier in column: {column.name}. Max Number: {max_val}. Min Number: {min_val}' + Style.RESET_ALL, stacklevel=2)
        return 'INT'
    elif min_val in range(-2_147_483_648, 2_147_483_648) and max_val in range(-2_147_483_648, 2_147_483_648):
        return 'INT'
    elif (min_val in range(-2_147_483_648, 2_147_483_648) or max_val in range(-2_147_483_648, 2_147_483_648)) and (min_val not in range(-2_147_483_648, 2_147_483_648) or max_val not in range(-2_147_483_648, 2_147_483_648)):
        warnings.warn(Fore.YELLOW + f'\nWARNING: Potential outlier in column: {column.name}. Max Number: {max_val}. Min Number: {min_val}' + Style.RESET_ALL, stacklevel=2)
        return 'BIGINT'
    else:
        return 'BIGINT'

def RunQuery(df,query:str,conn, ChunkSize:int = 20_000, NoChunking:bool = False, BarDesc:str = 'Processing rows', BarColor:str='green') -> None:
    df  = df.astype(object).where(pd.notnull(df), None)
    
    cursor = conn.cursor()
    
    cursor.fast_executemany = True
    
    if NoChunking:
        total_rows = len(df)
        pbar = tqdm(range(total_rows), desc=BarDesc, unit="row", colour=BarColor)
        cursor.executemany(query, df.values.tolist())
        conn.commit()
        pbar.update(total_rows)
        pbar.set_postfix_str(Fore.GREEN + f"Completed: Processed {total_rows}/{total_rows} rows" + Style.RESET_ALL)
        pbar.close()
        return
    
    # Get the total number of rows for the progress bar
    total_rows = len(df)
    
    if ChunkSize > len(df):
        ChunkSize = len(df)
    
    # Use tqdm to create a progress bar
    with tqdm(total=total_rows, desc=BarDesc, unit="row", colour=BarColor) as pbar:
        rows_list = df.values.tolist()
        for i in range(0, len(rows_list), ChunkSize):
            batch = rows_list[i:i + ChunkSize]
            
            # Execute the query for the current batch
            cursor.executemany(query, batch)
            conn.commit()
            
            # Update the progress bar for each iteration
            pbar.update(ChunkSize)
            
            # Update the progress bar description dynamically
            processed_rows = min(pbar.n, total_rows)
            pbar.set_postfix_str(f"Processed {processed_rows}/{total_rows} rows")
            
        # If successfully completed, set the progress bar to green
        pbar.set_postfix_str(Fore.GREEN + f"Completed: Processed {total_rows}/{total_rows} rows" + Style.RESET_ALL)
        
    # Close the progress bar after completion or error
    pbar.close()
    return
  
class Query:
    __slots__ = ('df','table','save','path')
    
    def __init__(self, df, table:str, save:bool = False, path:str = None):
        self.df     = df
        self.table  = table
        self.save   = save
        self.path   = path
        if not isinstance(self.table, str):
            raise TypeError('Table value must be a str')
                
    def SaveQuery(self, query, query_type):
        if self.save:
            if query_type == 'insert':
                path = f'InsertQuery - {self.table}.sql'
            elif query_type == 'update':
                path = f'UpdateQuery - {self.table}.sql'
            elif query_type == 'CreateTable':
                path = f'CreateTable - {self.table}.sql'
            else:
                raise ValueError('Invalid query type. Cannot Save')

            if self.path:
                path = self.path + path
            with open(path, 'w') as sql_file:
                sql_file.write(query)    
                
    '''
    The CreateTable method is intended to HELP create a more accurate database schema (for SQL SERVER) 
    based on a pandas dataframe.However the schema still may not be 100% as expected and should
    be checked and changed if needed. 
    
    THIS CreateTable METHOD IS MEANT TO BE USED WHEN DEVELOPING THE SQL TABLE. 
    IT IS NOT INTENDED FOR A PRODUCTION ENVIRONMENT.
    '''
   
    def CreateTable(self,
                primary: list = None,
                primaryName: str = None,
                foreign: list = None,
                foreignName: str = None,
                foreignTable: str = None,
                foreignRelated: list = None,
                unique: list = None,
                uniqueName: str = None,
                charbuff: int = 10
                ) -> str:  
        
        if foreign and not foreignTable:
            raise ValueError('foreignTable must not be None if a foreign key is to be added')

        # Define a mapping of pandas data types to SQL Server data types
        sql_data_types = {
            'object': 'VARCHAR',
            'datetime64[ns]': 'DATETIME',
            'bool': 'BIT',
        }

        # Get the columns and their data types from the DataFrame
        columns = self.df.columns
        dtypes = self.df.dtypes

        # Create the CREATE TABLE statement
        create_table_query = f"CREATE TABLE [{self.table}] (\n"

        for column, dtype in zip(columns, dtypes):
            sql_type = sql_data_types.get(str(dtype), 'VARCHAR(max)')  # defaulting to VARCHAR(max) if no datatype is matched
            if dtype == 'object':
                max_length = self.df[column].astype(str).apply(len).max() + charbuff
                create_table_query += f"    [{column}] {sql_type}({max_length}),\n"
            elif 'float' in str(dtype):
                max_digits = self.df[column].apply(DecimalCount)
                # Find the maximum values across all rows
                max_before = max_digits['Max_Before'].max()
                max_after = max_digits['Max_After'].max()
                create_table_query += f"    [{column}] DECIMAL({max_before + max_after},{max_after}),\n"
            elif 'int' in str(dtype):
                int_type = IntType(self.df[column])
                create_table_query += f"    [{column}] [{int_type}],\n"
            else:
                create_table_query += f"    [{column}] {sql_type},\n"
            if (primary != None and column in primary) or column == primary: #ensures that primary key columns are not null
                create_table_query = create_table_query[:-2]
                create_table_query += " NOT NULL,\n"

        create_table_query = create_table_query.rstrip(',\n') + "\n);"

        # Adding primary key(s) if needed
        if primary is not None:
            primary_key_constraint = f'CONSTRAINT [{primaryName}] PRIMARY KEY ' if primaryName else 'PRIMARY KEY '
            primary_keys = ", ".join([f'[{key}]' for key in primary])
            create_table_query += f'\nALTER TABLE [{self.table}]\nADD {primary_key_constraint}({primary_keys});\n'

        # Adding foreign key(s) if needed
        if foreign is not None and foreignTable is not None and foreignRelated is not None:
            foreign_key_constraint = f'CONSTRAINT [{foreignName}] FOREIGN KEY ' if foreignName else 'FOREIGN KEY '
            foreign_keys = ", ".join([f'[{key}]' for key in foreign])
            foreign_related_keys = ", ".join([f'[{key}]' for key in foreignRelated])
            create_table_query += f'\nALTER TABLE [{self.table}]\nADD {foreign_key_constraint}({foreign_keys})\nREFERENCES [{foreignTable}] ({foreign_related_keys});\n'

        # Adding unique key(s) if needed
        if unique is not None:
            unique_constraint = f'CONSTRAINT [{uniqueName}] UNIQUE ' if uniqueName else 'UNIQUE '
            unique_keys = ", ".join([f'[{key}]' for key in unique])
            create_table_query += f'\nALTER TABLE [{self.table}]\nADD {unique_constraint}({unique_keys});\n'
        
        self.SaveQuery(create_table_query, 'CreateTable')

        return create_table_query  
                
    def Insert(self) -> str:
        columns_str = '\n\t,'.join([f'[{col}]' for col in self.df.columns.tolist()])
        
        query = f'''
    INSERT INTO [{self.table}]
    (
    {columns_str}
        )
    VALUES 
        ({('?,' * len(self.df.columns.tolist()))[:-1]});
        '''
        self.SaveQuery(query, 'insert')
        return query
    
    def Update(self, where:list) -> str:
        if not isinstance(where, list):
            raise TypeError('Where value must be a list')

        # Removing columns that should only be in the where clause and add = ? for each column
        columns = '\n\t,'.join([f'[{value}] = ?' for value in self.df.columns.tolist() if value not in where])

        where_clause = '\n\tAND\n\t'.join([f'[{value}] = ?' for value in where]) # Making where clause

        query = f'''
        UPDATE [{self.table}]
        SET 
        {columns}
        WHERE 
        {where_clause};
        '''
        self.SaveQuery(query, 'update')
        return query