from setuptools import setup, find_packages


REQUIRES = ['SQLAlchemy>=1.0.9']
exec(open('marcottimls/version.py').read())

setup(
    name='marcotti-mls',
    version=__version__,
    packages=find_packages(),
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'dbsetup = marcottimls.tools.dbsetup:main'
        ]
    },
    url='https://github.com/soccermetrics/marcotti-mls',
    license='MIT',
    author='Soccermetrics Research',
    author_email='info@soccermetrics.net',
    keywords=['soccer', 'football', 'soccer analytics', 'data modeling', 'MLS'],
    install_requires=REQUIRES,
    extras_require={
        'PostgreSQL': ['psycopg2>=2.5.1'],
        'MySQL': ['mysql-python>=1.2.3'],
        'MSSQL': ['pyodbc>=3.0'],
        'Oracle': ['cx_oracle>=5.0'],
        'Firebird': ['fdb>=1.6']
    },
    tests_require=['pytest>=2.8.2'],
    description='Software library for creating and querying football databases specific to Major League Soccer',
    long_description=open('README.md').read()
)
