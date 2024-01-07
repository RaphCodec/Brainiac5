import pandas as pd
import numpy as np

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

def update(columns:list, table:str, where:list):
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
    
    if len(where) > 1:
        where = ' and, '.join([value + ' = ?' for value in where])
    else:
        where = where[0] + ' = ?'
    
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
    
    
insert_qry = insert(list(df.columns), 'Table')
print(insert_qry)
update_qry = update(list(df.columns), 'Table', ['Name'])
print(update_qry)

