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

    appearances = Column(Integer, CheckConstraint('appearances >= 0'), default=0)
    substituted = Column(Integer, CheckConstraint('substituted >= 0'), default=0)
    minutes = Column(Integer, CheckConstraint('minutes >= 0'), default=0)
    yellows = Column(Integer, CheckConstraint('yellows >= 0'), default=0)
    reds = Column(Integer, CheckConstraint('reds >= 0'), default=0)
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

    goals_total = Column(Integer, CheckConstraint('goals_total >= 0'), default=0)
    goals_headed = Column(Integer, CheckConstraint('goals_headed >= 0'), default=0)
    goals_freekick = Column(Integer, CheckConstraint('goals_freekick >= 0'), default=0)
    goals_in_area = Column(Integer, CheckConstraint('goals_in_area >= 0'), default=0)
    goals_out_area = Column(Integer, CheckConstraint('goals_out_area >= 0'), default=0)
    goals_winners = Column(Integer, CheckConstraint('goals_winners >= 0'), default=0)
    goals_penalty = Column(Integer, CheckConstraint('goals_penalty >= 0'), default=0)
    penalties_taken = Column(Integer, CheckConstraint('penalties_taken >= 0'), default=0)
    assists_total = Column(Integer, CheckConstraint('assists_total >= 0'), default=0)
    assists_deadball = Column(Integer, CheckConstraint('assists_deadball >= 0'), default=0)
    shots_total = Column(Integer, CheckConstraint('shots_total >= 0'), default=0)
    fouls_total = Column(Integer, CheckConstraint('fouls_total >= 0'), default=0)


class GoalkeeperStats(CommonStats):
    """
    Data model of season statistics for goalkeepers.
    """
    __tablename__ = 'gk_stats'
    __mapper_args__ = {'polymorphic_identity': 'goalkeeper'}

    id = Column(Integer, ForeignKey('common_stats.id'), primary_key=True)

    wins = Column(Integer, CheckConstraint('wins >= 0'), default=0)
    draws = Column(Integer, CheckConstraint('draws >= 0'), default=0)
    losses = Column(Integer, CheckConstraint('losses >= 0'), default=0)
    goals_allowed = Column(Integer, CheckConstraint('goals_allowed >= 0'), default=0)
    shots_allowed = Column(Integer, CheckConstraint('shots_allowed >= 0'), default=0)
    clean_sheets = Column(Integer, CheckConstraint('clean_sheets >= 0'), default=0)


class LeagueCompetitionPoints(BaseSchema):
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
