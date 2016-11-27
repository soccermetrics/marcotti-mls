import numpy as np

from marcottimls.lib.base import Analytics
from marcottimls.models import *


class ValuationAnalytics(Analytics):

    def __init__(self, session, competition, season):
        super(ValuationAnalytics, self).__init__(session)
        self.competition_name = competition
        self.season_name = season

    @property
    def competition(self):
        return self.session.query(Competitions).filter_by(name=self.competition_name).one()

    @property
    def season(self):
        return self.session.query(Seasons).filter_by(name=self.season_name).one()

    @property
    def comp_season(self):
        return self.session.query(CompetitionSeasons).join(Competitions).join(Seasons). \
            filter(Competitions.name == self.competition_name, Seasons.name == self.season_name).one()

    def league_records(self, model):
        return self.session.query(model).filter(model.competition_id == self.competition.id,
                                                model.season_id == self.season.id)

    def league_stats(self, model, statistic):
        return [getattr(rec, statistic) for rec in self.league_records(model)]

    def club_stats(self, model, club, statistic):
        records = self.league_records(model).filter(model.club_id == club.id)
        return [getattr(rec, statistic) for rec in records]

    def value(self, player):
        if player.primary_position == PositionType.goalkeeper:
            model = GoalkeeperStats
            metrics = ['minutes', 'goals_allowed', 'clean_sheets']
        else:
            model = FieldPlayerStats
            metrics = ['minutes', 'goals_total', 'assists_total']
        player_records = self.league_records(model).filter(model.player_id == player.id)

        # metrics relative to team
        team_value = []
        league_value = []
        for metric in metrics:
            try:
                league_max = max([getattr(rec, metric) for rec in self.league_records(model)])
                if metric == 'minutes':
                    tvals = [float(getattr(rec, metric))/(rec.appearances*90) for rec in player_records]
                    cvals = [float(getattr(rec, metric))/league_max for rec in player_records]
                elif metric == 'goals_allowed':
                    tvals = [(sum(self.club_stats(model, rec.club, metric)) - float(getattr(rec, metric))) / sum(
                        self.club_stats(model, rec.club, metric)) for rec in player_records]
                    cvals = [float(league_max - getattr(rec, metric))/league_max for rec in player_records]
                else:
                    tvals = [float(getattr(rec, metric))/sum(self.club_stats(model, rec.club, metric))
                             for rec in player_records]
                    cvals = [float(getattr(rec, metric))/league_max for rec in player_records]
                team_value.append(sum(tvals))
                league_value.append(sum(cvals))
            except ZeroDivisionError:
                team_value.append(0.0)
                league_value.append(sum(cvals))
        return np.sqrt(np.mean(np.square(league_value)))
