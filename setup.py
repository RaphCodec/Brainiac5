from setuptools import setup, find_packages

VERSION = '0.0.1' 
DESCRIPTION = 'PYODBC and Pandas helper module'
LONG_DESCRIPTION = 'Library to help with commonn ETL tasks with sql and pandas'

# Setting up
setup(
       # the name must match the folder name 'verysimplemodule'
        name="brainiac5", 
        version=VERSION,
        author="Rapahel Cilfton (Github: RaphCodeC)",
        author_email="",
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        packages=find_packages(include=['brainiac5']),
        install_requires=['pandas>=2.0.3','pyodbc>=4.0.39', 'tqdm>=4.66.1'], # add any additional packages that 
        # needs to be installed along with your package. Eg: 'caer'
        
        keywords=['python', 'ETL'],
        classifiers= [
            "Intended Audience :: Developers",
            "Programming Language :: Python :: 3",
            "Operating System :: Microsoft :: Windows",
        ]
)