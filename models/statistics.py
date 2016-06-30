from sqlalchemy import Column, Integer, String, ForeignKey, Sequence, Index, ForeignKeyConstraint
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import CheckConstraint

from models.common import BaseSchema


class CommonStats(BaseSchema):
    """
    Data model of common season statistics for football players.
    """
    __tablename__ = 'common_stats'
    __table_args__ = (
        ForeignKeyConstraint(
            ['competition_id', 'season_id'],
            ['competition_seasons.competition_id', 'competition_seasons.season_id'],
        ),
    )

    id = Column(Integer, Sequence('stat_id_seq', start=100000), primary_key=True)

    appearances = Column(Integer)
    substituted = Column(Integer)
    minutes = Column(Integer)
    yellows = Column(Integer)
    reds = Column(Integer)
    type = Column(String)

    player_id = Column(Integer, ForeignKey('players.id'))
    club_id = Column(Integer, ForeignKey('clubs.id'))
    competition_id = Column(Integer)
    season_id = Column(Integer)

    player = relationship('Players', backref=backref('stats'))
    club = relationship('Clubs', backref=backref('stats'))
    comp_season = relationship('CompetitionSeasons', backref=backref('player_stats'))

    Index('player_stats_indx', 'player_id', 'club_id', 'competition_id', 'season_id')

    __mapper_args__ = {
        'polymorphic_identity': 'common',
        'polymorphic_on': type
    }


class FieldPlayerStats(CommonStats):
    """
    Data model of season statistics for field players.
    """
    __tablename__ = 'field_stats'
    __mapper_args__ = {'polymorphic_identity': 'field'}

    id = Column(Integer, ForeignKey('common_stats.id'), primary_key=True)

    goals_total = Column(Integer)
    goals_headed = Column(Integer)
    goals_freekick = Column(Integer)
    goals_in_area = Column(Integer)
    goals_out_area = Column(Integer)
    goals_winners = Column(Integer)
    goals_penalty = Column(Integer)
    penalties_taken = Column(Integer)
    assists_total = Column(Integer)
    assists_deadball = Column(Integer)
    shots_total = Column(Integer)
    fouls_total = Column(Integer)


class GoalkeeperStats(CommonStats):
    """
    Data model of season statistics for goalkeepers.
    """
    __tablename__ = 'gk_stats'
    __mapper_args__ = {'polymorphic_identity': 'goalkeeper'}

    id = Column(Integer, ForeignKey('common_stats.id'), primary_key=True)

    wins = Column(Integer)
    draws = Column(Integer)
    losses = Column(Integer)
    goals_allowed = Column(Integer)
    shots_allowed = Column(Integer)
    clean_sheets = Column(Integer)


class LeaguePoints(BaseSchema):
    """
    Data model of points earned in league competitions.
    """
    __tablename__ = "league_points"
    __table_args__ = (
        ForeignKeyConstraint(
            ['competition_id', 'season_id'],
            ['competition_seasons.competition_id', 'competition_seasons.season_id'],
        ),
    )

    id = Column(Integer, Sequence('leaguept_id_seq', start=10000), primary_key=True)

    played = Column(Integer, CheckConstraint('played > 0'))
    points = Column(Integer, CheckConstraint('points >= 0'))

    club_id = Column(Integer, ForeignKey('clubs.id'))
    competition_id = Column(Integer)
    season_id = Column(Integer)

    comp_season = relationship('CompetitionSeasons', backref=backref('team_points'))
