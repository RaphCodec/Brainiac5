from setuptools import find_packages, setup

setup(
    name='ETLFlow',
    packages=find_packages(include=['ETLFlow']),
    version='0.1.0',
    description='Python helper library desinged to work with Pandas and PYODBC. Main use is to help with ETL work.',
    author='RaphCodec',
    install_requires=['pandas==2.0.3','pyodbc==4.0.39'],
    setup_requires=['pytest-runner'],
    tests_require=['pytest==4.4.1'],
    test_suite='tests',
)