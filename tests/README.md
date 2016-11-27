Marcotti-MLS Test Suite
=======================

Marcotti-MLS will have a test suite that contains unit and functional tests on the database schema.  It is useful for
demonstrating the specifications and use cases of the schema.
 
The test suite uses [py.test](http://www.pytest.org) and a server-based database (PostgreSQL by default).  A blank 
database named `test-marcotti-db` must be created before the tests are run.
 
Use the following command from the top-level directory of the repository to run the tests:
 
        $ py.test
        
        