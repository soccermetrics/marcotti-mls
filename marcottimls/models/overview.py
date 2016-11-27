from datetime import date

from sqlalchemy import (case, select, cast, Column, Integer, Date,
                        String, Sequence, ForeignKey, Unicode)
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method
from sqlalchemy.orm import relationship, backref

import marcottimls.models.enums as enums
from marcottimls.models.common import BaseSchema


class Countries(BaseSchema):
    """
    Countries data model.

    Countries are defined as FIFA-affiliated national associations.
    """
    __tablename__ = "countries"

    id = Column(Integer, Sequence('country_id_seq', start=100), primary_key=True)

    name = Column(Unicode(60))
    confederation = Column(enums.ConfederationType.db_type())

    def __repr__(self):
        return u"<Country(id={0}, name={1}, confed={2})>".format(self.id, self.name,
                                                                 self.confederation.value).encode('utf-8')


class Years(BaseSchema):
    """
    Years data model.
    """
    __tablename__ = "years"

    id = Column(Integer, Sequence('year_id_seq', start=100), primary_key=True)
    yr = Column(Integer, unique=True)

    def __repr__(self):
        return "<Year(yr={0})>".format(self.yr)


class Seasons(BaseSchema):
    """
    Seasons data model.
    """
    __tablename__ = "seasons"

    id = Column(Integer, Sequence('season_id_seq', start=100), primary_key=True)

    start_year_id = Column(Integer, ForeignKey('years.id'))
    end_year_id = Column(Integer, ForeignKey('years.id'))

    start_year = relationship('Years', foreign_keys=[start_year_id])
    end_year = relationship('Years', foreign_keys=[end_year_id])

    @hybrid_property
    def name(self):
        """
        List year(s) that make up season.  Seasons over calendar year will be of form YYYY;
        seasons over two years will be of form YYYY-YYYY.
        """
        if self.start_year.yr == self.end_year.yr:
            return "{0}".format(self.start_year.yr)
        else:
            return "{0}-{1}".format(self.start_year.yr, self.end_year.yr)

    @name.expression
    def name(cls):
        """
        List year(s) that make up season.  Seasons over calendar year will be of form YYYY;
        seasons over two years will be of form YYYY-YYYY.

        This expression allows `name` to be used as a query parameter.
        """
        yr1 = select([Years.yr]).where(cls.start_year_id == Years.id).as_scalar()
        yr2 = select([Years.yr]).where(cls.end_year_id == Years.id).as_scalar()
        return cast(yr1, String) + case([(yr1 == yr2, '')], else_='-'+cast(yr2, String))

    @hybrid_property
    def reference_date(self):
        """
        Define the reference date that is used to calculate player ages.

        +------------------------+---------------------+
        | Season type            | Reference date      |
        +========================+=====================+
        | European (Split years) | 30 June             |
        +------------------------+---------------------+
        | Calendar-year          | 31 December         |
        +------------------------+---------------------+

        :return: Date object that expresses reference date.
        """
        if self.start_year.yr == self.end_year.yr:
            return date(self.end_year.yr, 12, 31)
        else:
            return date(self.end_year.yr, 6, 30)

    def __repr__(self):
        return "<Season({0})>".format(self.name)


class Competitions(BaseSchema):
    """
    Competitions common data model.
    """
    __tablename__ = 'competitions'

    id = Column(Integer, Sequence('competition_id_seq', start=1000), primary_key=True)

    name = Column(Unicode(80))
    level = Column(Integer)
    discriminator = Column('type', String(20))

    __mapper_args__ = {
        'polymorphic_identity': 'competitions',
        'polymorphic_on': discriminator
    }


class DomesticCompetitions(Competitions):
    """
    Domestic Competitions data model, inherited from Competitions model.
    """
    __mapper_args__ = {'polymorphic_identity': 'domestic'}
    country_id = Column(Integer, ForeignKey('countries.id'))
    country = relationship('Countries', backref=backref('competitions'))

    def __repr__(self):
        return u"<DomesticCompetition(name={0}, country={1}, level={2})>".format(
            self.name, self.country.name, self.level).encode('utf-8')


class InternationalCompetitions(Competitions):
    """
    International Competitions data model, inherited from Competitions model.
    """
    __mapper_args__ = {'polymorphic_identity': 'international'}

    confederation = Column(enums.ConfederationType.db_type())

    def __repr__(self):
        return u"<InternationalCompetition(name={0}, confederation={1})>".format(
            self.name, self.confederation.value).encode('utf-8')


class CompetitionSeasons(BaseSchema):
    """
    Data model for a season's league competition. (Regular season only)
    """
    __tablename__ = 'competition_seasons'

    competition_id = Column(Integer, ForeignKey('competitions.id'), primary_key=True)
    season_id = Column(Integer, ForeignKey('seasons.id'), primary_key=True)

    start_date = Column(Date)
    end_date = Column(Date)
    matchdays = Column(Integer)

    competition = relationship('Competitions')
    season = relationship('Seasons')

    @hybrid_property
    def weeks(self):
        """
        Number of calendar weeks in competition's season.

        :return: Number of weeks between start and end dates of season
        """
        return ((self.end_date - self.start_date).days + 1) / 7

    def __repr__(self):
        return u"<CompetitionSeason(name={0}, season={1}, start={2}, end={3}, matches={4})>".format(
            self.competition.name, self.season.name, self.start_date.isoformat(),
            self.end_date.isoformat(), self.matchdays).encode('utf-8')

    def __unicode__(self):
        return u"<CompetitionSeason(name={0}, season={1}, start={2}, end={3}, matches={4})>".format(
            self.competition.name, self.season.name, self.start_date.isoformat(),
            self.end_date.isoformat(), self.matchdays)


class Clubs(BaseSchema):
    """
    Football club data model.
    """
    __tablename__ = 'clubs'

    id = Column(Integer, Sequence('club_id_seq', start=10000), primary_key=True)

    name = Column(Unicode(60))
    symbol = Column(String(5))

    country_id = Column(Integer, ForeignKey('countries.id'))
    country = relationship('Countries', backref=backref('clubs'))

    def __repr__(self):
        return "<Club(name={0}, country={1})>".format(self.name, self.country.name)

    def __unicode__(self):
        return u"<Club(name={0}, country={1})>".format(self.name, self.country.name)


class Persons(BaseSchema):
    """
    Persons common data model.   This model is subclassed by other Personnel data models.
    """
    __tablename__ = 'persons'

    person_id = Column(Integer, Sequence('person_id_seq', start=100000), primary_key=True)
    first_name = Column(Unicode(40))
    known_first_name = Column(Unicode(40))
    middle_name = Column(Unicode(40))
    last_name = Column(Unicode(40))
    second_last_name = Column(Unicode(40))
    nick_name = Column(Unicode(40))
    birth_date = Column(Date)
    order = Column(enums.NameOrderType.db_type(), default=enums.NameOrderType.western)
    type = Column(String)

    country_id = Column(Integer, ForeignKey('countries.id'))
    country = relationship('Countries', backref=backref('persons'))

    __mapper_args__ = {
        'polymorphic_identity': 'persons',
        'polymorphic_on': type
    }

    @hybrid_property
    def full_name(self):
        """
        The person's commonly known full name, following naming order conventions.

        If a person has a nickname, that name becomes the person's full name.

        If a person has an alternate first name by which he/she is otherwise known, that name
        becomes part of the full name.

        :return: Person's full name.
        """
        if self.nick_name is not None:
            return self.nick_name
        else:
            if self.order == enums.NameOrderType.western:
                return u"{} {}".format(self.known_first_name or self.first_name, self.last_name)
            elif self.order == enums.NameOrderType.middle:
                return u"{} {} {}".format(self.known_first_name or self.first_name, self.middle_name, self.last_name)
            elif self.order == enums.NameOrderType.eastern:
                return u"{} {}".format(self.last_name, self.first_name)

    @full_name.expression
    def full_name(cls):
        """
        The person's commonly known full name, following naming order conventions.

        If a person has a nickname, that name becomes the person's full name.

        If a person has an alternate first name by which he/she is otherwise known, that name
        becomes part of the full name.

        :return: Person's full name.
        """
        return case(
            [(cls.nick_name != None, cls.nick_name)],
            else_=case(
                [
                    (cls.order == enums.NameOrderType.middle,
                     case([(cls.known_first_name != None, cls.known_first_name)], else_=cls.first_name) +
                        u' ' + cls.middle_name + u' ' + cls.last_name),
                    (cls.order == enums.NameOrderType.eastern, cls.last_name + u' ' + cls.first_name)
                ],
                else_=case(
                    [(cls.known_first_name != None, cls.known_first_name)],
                    else_=cls.first_name) + u' ' + cls.last_name))

    @hybrid_property
    def official_name(self):
        """
        The person's legal name, following naming order conventions and with middle names included.

        :return: Person's legal name.
        """
        if self.order == enums.NameOrderType.eastern:
            return u"{} {}".format(self.last_name, self.first_name)
        else:
            return u" ".join([getattr(self, field) for field in
                              ['first_name', 'middle_name', 'last_name', 'second_last_name']
                              if getattr(self, field) is not None])

    @official_name.expression
    def official_name(cls):
        """
        The person's legal name, following naming order conventions and with middle names included.

        :return: Person's legal name.
        """
        return case(
            [(cls.order == enums.NameOrderType.eastern, cls.last_name + u' ' + cls.first_name)],
            else_=(
                case([cls.first_name != None, cls.first_name + u' '], else_=u'') +
                case([cls.middle_name != None, cls.middle_name + u' '], else_=u'') +
                case([cls.last_name != None, cls.last_name + u' '], else_=u'') +
                case([cls.second_last_name != None, cls.second_last_name], else_=u''))
        )

    @hybrid_method
    def exact_age(self, reference):
        """
        Player's exact age (years + days) relative to a reference date.

        :param reference: Date object of reference date.
        :return: Player's age expressed as a (Year, day) tuple
        """
        delta = reference - self.birth_date
        years = int(delta.days/365.25)
        days = int(delta.days - years*365.25 + 0.5)
        return years, days

    @hybrid_method
    def age(self, reference):
        """
        Player's age relative to a reference date.

        :param reference: Date object of reference date.
        :return: Integer value of player's age.
        """
        delta = reference - self.birth_date
        return int(delta.days/365.25)

    @age.expression
    def age(cls, reference):
        """
        Person's age relative to a reference date.

        :param reference: Date object of reference date.
        :return: Integer value of person's age.
        """
        return cast((reference - cls.birth_date)/365.25 - 0.5, Integer)

    def __repr__(self):
        return u"<Person(name={}, country={}, DOB={})>".format(
            self.full_name, self.country.name, self.birth_date.isoformat()).encode('utf-8')


class Players(Persons):
    """
    Players data model.

    Inherits Persons model.
    """
    __tablename__ = 'players'
    __mapper_args__ = {'polymorphic_identity': 'players'}

    id = Column(Integer, Sequence('player_id_seq', start=100000), primary_key=True)
    person_id = Column(Integer, ForeignKey('persons.person_id'))

    primary_position = Column(enums.PositionType.db_type(), default=enums.PositionType.unknown)
    secondary_position = Column(enums.PositionType.db_type())

    @hybrid_property
    def position(self):
        return '/'.join([pos.value for pos in [self.primary_position, self.secondary_position] if pos])

    def __repr__(self):
        return u"<Player(name={}, DOB={}, country={}, position={})>".format(
            self.full_name, self.birth_date.isoformat(), self.country.name, self.position).encode('utf-8')

    def __unicode__(self):
        return u"<Player(name={}, DOB={}, country={}, position={})>".format(
            self.full_name, self.birth_date.isoformat(), self.country.name, self.position)
