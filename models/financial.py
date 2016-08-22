from sqlalchemy import Column, Integer, String, Sequence, ForeignKey, ForeignKeyConstraint, Boolean
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
    gen_adidas = Column(Boolean, default=False)

    def __repr__(self):
        return u"<PlayerDraft(name={0}, year={1}, round={2}, selection={3}, generation_adidas={4})>".format(
            self.player.full_name, self.year.yr, self.round, self.selection, self.gen_adidas).encode('utf-8')

    def __unicode__(self):
        return u"<PlayerDraft(name={0}, year={1}, round={2}, selection={3}, generation_adidas={4})>".format(
            self.player.full_name, self.year.yr, self.round, self.selection, self.gen_adidas)


class PlayerSalaries(BaseSchema):
    """
    Player salary data model.
    """
    __tablename__ = 'salaries'
    __table_args__ = (
        ForeignKeyConstraint(
            ['competition_id', 'season_id'],
            ['competition_seasons.competition_id', 'competition_seasons.season_id'],
        ),
    )

    id = Column(Integer, Sequence('salary_id_seq', start=10000), primary_key=True)

    base_salary = Column(Integer, CheckConstraint('base_salary >= 0'), doc="Base salary in cents")
    avg_guaranteed = Column(Integer, CheckConstraint('avg_guaranteed >= 0'),
                            doc="Average annualized guaranteed compensation in cents")

    player_id = Column(Integer, ForeignKey('players.id'))
    club_id = Column(Integer, ForeignKey('clubs.id'))
    competition_id = Column(Integer)
    season_id = Column(Integer)

    player = relationship('Players', backref=backref('salaries'))
    club = relationship('Clubs', backref=backref('payroll'))
    comp_season = relationship('CompetitionSeasons', backref=backref('payroll'))

    def __repr__(self):
        return u"<PlayerSalary(name={0}, club={1}, competition={2}, season={3}, base={4:.2f}, " \
               u"guaranteed={5:.2f})>".format(self.player.full_name, self.club.name,
                                              self.comp_season.competition.name, self.comp_season.season.name,
                                              self.base_salary/100.00, self.avg_guaranteed/100.00).encode('utf-8')

    def __unicode__(self):
        return u"<PlayerSalary(name={0}, club={1}, competition={2}, season={3}, base={4:.2f}, " \
               u"guaranteed={5:.2f})>".format(self.player.full_name, self.club.name,
                                              self.comp_season.competition.name, self.comp_season.season.name,
                                              self.base_salary / 100.00, self.avg_guaranteed / 100.00)


class PartialTenures(BaseSchema):
    """
    Data model that captures player's partial-season tenure at a club.
    """
    __tablename__ = 'partials'
    __table_args__ = (
        ForeignKeyConstraint(
            ['competition_id', 'season_id'],
            ['competition_seasons.competition_id', 'competition_seasons.season_id'],
        ),
    )

    id = Column(Integer, Sequence('partial_id_seq', start=10000), primary_key=True)

    start_week = Column(Integer, CheckConstraint('start_week > 0'))
    end_week = Column(Integer, CheckConstraint('end_week > 0'))

    player_id = Column(Integer, ForeignKey('players.id'))
    club_id = Column(Integer, ForeignKey('clubs.id'))
    competition_id = Column(Integer)
    season_id = Column(Integer)

    player = relationship('Players', backref=backref('partials'))
    club = relationship('Clubs', backref=backref('partials'))
    comp_season = relationship('CompetitionSeasons', backref=backref('partials'))

    def __repr__(self):
        return u"<PartialTenure(name={0}, club={1}, competition={2}, season={3}, " \
               u"start_week={4}, end_week={5})>".format(self.player.full_name, self.club.name,
                                                        self.comp_season.competition.name,
                                                        self.comp_season.season.name,
                                                        self.start_week, self.end_week).encode('utf-8')

    def __unicode__(self):
        return u"<PartialTenure(name={0}, club={1}, competition={2}, season={3}, " \
               u"start_week={4}, end_week={5})>".format(self.player.full_name, self.club.name,
                                                        self.comp_season.competition.name,
                                                        self.comp_season.season.name,
                                                        self.start_week, self.end_week)
