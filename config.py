class Config(object):
    """
    Base configuration class.  Contains one property that defines the database URI.

    This class is to be subclassed and its attributes defined therein.
    """

    @property
    def database_uri(self):
        return r'sqlite://{name}'.format(name=self.DBNAME) if self.DIALECT == 'sqlite' else \
            r'{dialect}://{user}:{passwd}@{host}:{port}/{name}'.format(
                dialect=self.DIALECT, user=self.DBUSER, passwd=self.DBPASSWD,
                host=self.HOSTNAME, port=self.PORT, name=self.DBNAME
            )
