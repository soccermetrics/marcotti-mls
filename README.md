Marcotti-MLS
============

**Marcotti-MLS** is a data schema used to create football databases. These databases capture statistical data used to
support football research activities unique to Major League Soccer (MLS).  These statistical data include historical
data such as the following:

* Complete biographic/demographic data of all players who participated in or were drafted by Major League Soccer.
* Base and supplemental salary data of all players offered a contract in Major League Soccer.
* Data on player entry paths into Major League Soccer, including drafts, allocations, and developmental, 
Homegrown, and Designated Player contracts.
* Tenure periods of players at Major League Soccer clubs, used to track in-season movements.
* Seasonal statistics of participating players, including minutes played, goals, assists, disciplinary 
records, and goalkeeper specific data.

The Marcotti-MLS data schema is made up of backend-independent SQLAlchemy objects.

## Installation

Marcotti-MLS is written in Python and uses the SQLAlchemy package heavily.

While not required, [virtualenv](https://pypi.python.org/pypi/virtualenv) is strongly recommended and
[virtualenvwrapper](https://pypi.python.org/pypi/virtualenvwrapper) is very convenient.

Installation instructions:

1. Setup a virtual environment, grab the latest Marcotti-MLS repo, and install it into the environment:

        $ cd /path/to/working/dir
        $ mkvirtualenv marcotti-mls
        (marcotti-mls) $ git clone git://github.com/soccermetrics/marcotti-mls.git
        (marcotti-mls) $ cd marcotti-mls
        (marcotti-mls) $ make install
    
2. Copy `local.skel` to `local.py` and populate it.  Alternative configuration
   settings can be created by subclassing `LocalConfig` and overwriting the attributes.
    
   ```python
   class LocalConfig(Config):
        # At a minimum, these variables must be defined.
        DIALECT = ''
        DBNAME = ''
        
        # For all other non-SQLite databases, these variables must be set.
        DBUSER = ''
        DBPASSWD = ''
        HOSTNAME = ''
        PORT = 5432
   ```
    
## Data Models

Unlike other data models in the Marcotti family, Marcotti-MLS is a single schema because it is only relevant to club 
football (in this case, a single league).  Marcotti-MLS does borrow common models from other Marcotti schema:

## Documentation

The [Marcotti-MLS wiki](https://github.com/soccermetrics/marcotti-mls/wiki) contains extensive user documentation of the 
package.

## License

(c) 2015-2016 Soccermetrics Research, LLC. Created under MIT license.  See `LICENSE` file for details.
