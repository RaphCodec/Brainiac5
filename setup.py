from setuptools import setup, find_packages

VERSION = '0.0.1' 
DESCRIPTION = 'PYODBC and Pandas helper module'
LONG_DESCRIPTION = 'Library to help with commonn ETL tasks with sql and pandas'

# Setting up
setup(
       # the name must match the folder name 'verysimplemodule'
        name="brainiac5", 
        url='https://github.com/RaphCodec/Brainiac5',
        version=VERSION,
        author="Rapahel Cilfton",
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        packages=find_packages(include=['brainiac5']),
        include_package_data=True,
        license='GNU GENERAL PUBLIC LICENSE',
        install_requires=['pandas>=2.0.3','pyodbc>=4.0.39', 'tqdm>=4.66.1'],
        keywords=['Extract', 'Trasform', 'Load', 'SQL', 'Data Transformation', 'Data', 'ETL'],
        classifiers= [
            "Intended Audience :: Developers",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
            "Programming Language :: Python :: 3.10",
            "Programming Language :: Python :: 3.11",
            "Operating System :: Microsoft :: Windows",
        ]
)