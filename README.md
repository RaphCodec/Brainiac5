# THIS PROJECT HAS BEEN ARCHIVED AS OF NOVEMBER 11TH, 2024.  THE PyPI PACKAGE ASSOCAITED WITH THIS PROJECT WILL BE REMOVED WITHIN THE FIRST QUARTER OF 2025.

# Brainiac5

Brainiac5 is a Python package designed to automate basic SQL tasks, allowing for streamlined data engineering development in python. This README provides an overview of the package's features, usage instructions, and planned enhancements.

## Features

- Automates basic SQL tasks in Python.
- Provides functionalities to handle SQL queries and data transformation.

## Usage

To use Brainiac5, simply install the package using pip (python 3.8+ officially supported):

```bash
pip install brainiac5
```

To try the latest development features:

```bash
pip install git+https://github.com/RaphCodec/Brainiac5.git
```

Then, you can import the necessary modules and start using the provided functionalities:

```python
import brainiac5 as b5
import pandas as pd

## Example usage. See Example and Template Folders for more
df = pd.read_csv(SOURCE)

#creating a query object to create all queries related to df and DEST_TABLE
query = b5.Query(df, DEST_TABLE)

#Making Create Table Query. Comment out for Production
ct = query.CreateTable(primary=['PK'], primaryName=f'PK__{DEST_TABLE}__ID')

#Make parameterized insert and update queries.
insert = query.Insert()
update = query.Update(where=['PK'])
```

## Planned Features

1. **Error Handling for Queries:** Enhance error handling mechanisms to provide informative feedback on query failures. 
2. **Query Functions Converted to Class Object:** Refactor query functions into a class object for improved organization and reusability. (on master)
3. **Additional Data Transformation Class Object:**
   - Implement a class object for data transformation, allowing for functionalities like normalizing pandas dataframes similar to SQL tables.
4. **Generate ETL Templates:** Introduce templates for Extract, Transform, Load (ETL) processes to streamline data pipeline development.
5. **Module to Replicate Access Databases:** Develop a module to replicate Access databases for migration or backup purposes.
6. **Error notification** Send emails to script admins. (in progress)
7. **Adding more Data Types** Adding more data types for CreateTable in Query class to imporve accuracy.

## Contribution

Contributions to Brainiac5 are welcome! Feel free to submit pull requests or open issues to suggest enhancements or report bugs.

