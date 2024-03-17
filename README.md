# Brainiac5

Brainiac5 is a Python package designed to automate basic SQL tasks, allowing for streamlined data engineering development in python. This README provides an overview of the package's features, usage instructions, and planned enhancements.

## Features

- Automates basic SQL tasks in Python.
- Provides functionalities to handle SQL queries and data transformation.

## Usage

To use Brainiac5, simply install the package using pip:

```bash
pip install brainiac5
```

Then, you can import the necessary modules and start using the provided functionalities:

```python
import brainiac5 as b5
import pandas as pd

## Example usage. See Example and Template Folders for more
df = pd.read_csv(SOURCE)
b5.CreateTable(df,DEST_TABLE,primary=['PK_Column'],primaryName=f'PK__{DEST_TABLE}__ID',saveQuery=True)
```

## Planned Features

1. **Error Handling for Queries:** Enhance error handling mechanisms to provide informative feedback on query failures.
2. **Query Functions Converted to Class Object:** Refactor query functions into a class object for improved organization and reusability.
3. **Additional Data Transformation Class Object:**
   - Implement a class object for data transformation, allowing for functionalities like normalizing pandas dataframes similar to SQL tables.
4. **Generate ETL Templates:** Introduce templates for Extract, Transform, Load (ETL) processes to streamline data pipeline development.
5. **Module to Replicate Access Databases:** Develop a module to replicate Access databases for migration or backup purposes.

## Contribution

Contributions to Brainiac5 are welcome! Feel free to submit pull requests or open issues to suggest enhancements or report bugs.
```