import os
from setuptools import setup


REQUIRES = ['SQLAlchemy>=1.0.9', 'pytest>=2.8.2']
exec(open('marcottimls/version.py').read())

setup(
    name='marcotti-mls',
    version=__version__,
    packages=['marcottimls', 'marcottimls.etl', 'marcottimls.lib', 'marcottimls.models'],
    package_data={'marcottimls': [os.path.join('data', 'countries.csv'), os.path.join('data', 'countries.json')]},
    url='https://github.com/soccermetrics/marcotti-mls',
    license='MIT',
    author='Soccermetrics Research',
    author_email='info@soccermetrics.net',
    keywords=['soccer', 'football', 'soccer analytics', 'data modeling', 'MLS'],
    install_requires=REQUIRES,
    description='Software library for creating and querying football databases specific to Major League Soccer',
    long_description="""
    Marcotti-MLS Database Library
    -----------------------------

    DESCRIPTION
    Marcotti-MLS is a software library used to create football databases that capture statistical data used to
    support football research activities unique to Major League Soccer (MLS).  These statistical data include
    historical data such as:

    * Complete biographic/demographic data of all players
    * Base and guaranteed salary data of all players offered a MLS contract
    * Data on player entry paths into Major League Soccer
    * In-season movements of players
    * Seasonal statistics of participating players, including minutes played, goals,
      assists, disciplinary records, and goalkeeper-specific data.

    Marcotti-MLS ships with a library that queries the formatted database to produce draft, payroll, and
    demographic analysis on the statistical data.

    LICENSE
    Marcotti-MLS is distributed under the MIT License.
    """
)
