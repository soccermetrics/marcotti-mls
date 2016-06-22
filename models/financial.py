from sqlalchemy import Column, Integer, String, Sequence, ForeignKey
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import CheckConstraint

from models.common import BaseSchema
import models.enums as enums


class AcquisitionPaths(BaseSchema):
    """
    MLS player acquisition data model.

    Captures **initial** entry path into league.
    """
    __tablename__ = 'acquisitions'

    player_id = Column(Integer, ForeignKey('players.id'), primary_key=True)
    year_id = Column(Integer, ForeignKey('years.id'), primary_key=True)

    path = Column(enums.AcquisitionType.db_type())
    discriminator = Column('type', String(20))

    player = relationship('Players', backref=backref('entry'))
    year = relationship('Years', backref=backref('acquisitions'))

    __mapper_args__ = {
        'polymorphic_identity': 'acquisitions',
        'polymorphic_on': discriminator
    }


class PlayerDrafts(AcquisitionPaths):
    """
    Player draft data model.
    """
    __mapper_args__ = {'polymorphic_identity': 'draft'}

    round = Column(Integer, CheckConstraint('round > 0'))
    selection = Column(Integer, CheckConstraint('selection > 0'))

    def __repr__(self):
        return "<PlayerDraft(name={0}, year={1}, round={2}, selection={3}>".format(
            self.player.full_name, self.year.name, self.round, self.selection).decode('utf-8')

    def __unicode__(self):
        return u"<PlayerDraft(name={0}, year={1}, round={2}, selection={3}>".format(
            self.player.full_name, self.year.name, self.round, self.selection)


class PlayerSalaries(BaseSchema):
    """
    MLS player salary data model.
    """
    __tablename__ = 'salaries'

    id = Column(Integer, Sequence('salary_id_seq', start=10000), primary_key=True)

    base_salary = Column(Integer, CheckConstraint('base_salary > 0'), doc="Base salary in cents")
    avg_guaranteed = Column(Integer, CheckConstraint('avg_guaranteed > 0'),
                            doc="Average annualized guaranteed compensation in cents")

    player_id = Column(Integer, ForeignKey('players.id'))
    club_id = Column(Integer, ForeignKey('clubs.id'))
    season_id = Column(Integer, ForeignKey('seasons.id'))

    player = relationship('Players', backref=backref('salaries'))
    club = relationship('Clubs', backref=backref('payroll'))
    season = relationship('Seasons', backref=backref('payroll'))

    def __repr__(self):
        return "<PlayerSalary(name={0}, club={1}, season={2}, base={3:.2f}, guaranteed={4:.2f})>".format(
            self.player.full_name, self.club.name, self.season.nane, self.base_salary/100.00,
            self.avg_guaranteed/100.00).encode('utf-8')

    def __unicode__(self):
        return u"<PlayerSalary(name={0}, club={1}, season={2}, base={3:.2f}, guaranteed={4:.2f})>".format(
            self.player.full_name, self.club.name, self.season.nane, self.base_salary/100.00,
            self.avg_guaranteed/100.00)


class PartialTenures(BaseSchema):
    """
    Data model that captures player's partial-season tenure at a club.
    """
    __tablename__ = 'partials'

    id = Column(Integer, Sequence('partial_id_seq', start=10000), primary_key=True)

    start_week = Column(Integer, CheckConstraint('start_week > 0'))
    end_week = Column(Integer, CheckConstraint('end_week > 0'))

    player_id = Column(Integer, ForeignKey('players.id'))
    club_id = Column(Integer, ForeignKey('clubs.id'))
    season_id = Column(Integer, ForeignKey('seasons.id'))

    player = relationship('Players', backref=backref('partials'))
    club = relationship('Clubs', backref=backref('partials'))
    season = relationship('Seasons', backref=backref('partials'))

    def __repr__(self):
        return "<PartialTenure(name={0}, club={1}, season={2}, start_week={3}, end_week={4})>".format(
            self.player.full_name, self.club.name, self.season.nane, self.start_week, self.end_week).encode('utf-8')

    def __unicode__(self):
        return u"<PartialTenure(name={0}, club={1}, season={2}, start_week={3}, end_week={4})>".format(
            self.player.full_name, self.club.name, self.season.nane, self.start_week, self.end_week)
