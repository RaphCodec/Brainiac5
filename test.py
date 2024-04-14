import json
import pandas as pd
import numpy as np
import pyodbc
import brainiac5 as b5
import unittest
from string import ascii_lowercase

#fucntion to connect to servers
def Connect(Server:str, Database:str):
    conn_str = f"""
        Driver=SQL Server Native Client 11.0;
        Server={ Server };
        Database={ Database };
        Trusted_connection=YES;
    """
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    return cursor, conn

class TestRunQueryError(unittest.TestCase):

    def test_num_insert_error(self):
        np.random.seed(42)

        # Create a DataFrame with random data
        data = {
            'A': np.random.randn(10),  # Random normal distribution
            'B': np.random.randint(0, 100, size=10),  # Random integers between 0 and 100
            'C': np.random.rand(10),  # Random floats between 0 and 1
        }

        df = pd.DataFrame(data) 
        
        #creating a query object to create all queries related to df and DEST_TABLE
        query = b5.Query(df, DEST_TABLE)
        
        #Making Create Table Query. Comment out for Production
        ct = query.CreateTable()
        
        #this line helps ensure there are no errors inserting NULLs into SQL
        df = df.astype(object).where(pd.notnull(df), None) 

        cursor, conn = Connect(DEST_SERVER, DEST_DATABASE)
        
        if cursor.tables(table=DEST_TABLE, tableType='TABLE').fetchone():
            # Execute the SQL command to drop the table
            cursor.execute(f'DROP TABLE {DEST_TABLE};')

            # Commit the transaction
            conn.commit()
            
        
        cursor.execute(ct)  
        conn.commit()
        
        df.at[8, 'B'] = 'error'
        # print(df)
        
        #making insert and update queries.
        insert = query.Insert()
        
        with self.assertRaises(pyodbc.ProgrammingError):
            b5.RunQuery(df,insert,conn,BarDesc='Inserting Data')
        
        conn.close()

    def test_string_insert_error(self):
        np.random.seed(42)

        # Create a DataFrame with random data
        data = {
            'A': np.random.randn(10),  # Random normal distribution
            'C': np.random.rand(10),  # Random floats between 0 and 1
        }
        
        letters = np.random.choice(list(ascii_lowercase), size=(10,3))
        data['B'] = [''.join(row) for row in letters]

        df = pd.DataFrame(data) 
        
        #creating a query object to create all queries related to df and DEST_TABLE
        query = b5.Query(df, DEST_TABLE)
        
        #Making Create Table Query. Comment out for Production
        ct = query.CreateTable()
        
        #this line helps ensure there are no errors inserting NULLs into SQL
        df = df.astype(object).where(pd.notnull(df), None) 

        cursor, conn = Connect(DEST_SERVER, DEST_DATABASE)
        
        if cursor.tables(table=DEST_TABLE, tableType='TABLE').fetchone():
            # Execute the SQL command to drop the table
            cursor.execute(f'DROP TABLE {DEST_TABLE};')

            # Commit the transaction
            conn.commit()
            
        
        cursor.execute(ct)  
        conn.commit()
        
        df.at[8, 'B'] = 'error'
        # print(df)
        
        #making insert and update queries.
        insert = query.Insert()
        
        with self.assertRaises(pyodbc.ProgrammingError):
            b5.RunQuery(df,insert,conn,BarDesc='Inserting Data')
        
        conn.close()


if __name__ == '__main__':
    CONFIG = "test.config.json"
    with open(CONFIG, 'r') as json_config:
        CONFIG_DICT = json.load(json_config)
        
    '''
    Loading CONFIG into variables to make script more readable.   
    Could use CONFIG_DICT['SOURCE'] throughout script if prefered.
    '''    
    SOURCE              = CONFIG_DICT['SOURCE']
    DEST_SERVER         = CONFIG_DICT['DEST_SERVER']
    DEST_DATABASE       = CONFIG_DICT['DEST_DATABASE']
    DEST_TABLE          = CONFIG_DICT['DEST_TABLE']

    unittest.main()