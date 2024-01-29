from setuptools import setup, find_packages

VERSION = '0.0.1' 
DESCRIPTION = 'PYODBC and Pandas helper module'
LONG_DESCRIPTION = 'Library to help with commonn ETL tasks with sql and pandas'

# Setting up
setup(
       # the name must match the folder name 'verysimplemodule'
        name="ETLFlow", 
        version=VERSION,
        author="Rapahel Cilfton",
        author_email="",
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        packages=find_packages(include=['ETLFlow']),
        install_requires=['pandas==2.0.3','pyodbc==4.0.39'], # add any additional packages that 
        # needs to be installed along with your package. Eg: 'caer'
        
        keywords=['python', 'ETL'],
        classifiers= [
            "Development Status :: 3 - Alpha",
            "Intended Audience :: Education",
            "Programming Language :: Python :: 2",
            "Programming Language :: Python :: 3",
            "Operating System :: MacOS :: MacOS X",
            "Operating System :: Microsoft :: Windows",
        ]
)