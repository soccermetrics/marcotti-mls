from sqlalchemy.sql import func

from models import *
from base import Analytics


class PayrollAnalytics(Analytics):
    """
    Carry out analysis on league and team payrolls over a specific competition and season.
    """

    def __init__(self, session, competition, season):
        super(PayrollAnalytics, self).__init__(session)
        self.competition_name = competition
        self.season_name = season

    @property
    def competition(self):
        return self.session.query(Competitions).filter_by(name=self.competition_name).one()

    @property
    def comp_season(self):
        return self.session.query(CompetitionSeasons).join(Competitions).join(Seasons). \
            filter(Competitions.name == self.competition_name, Seasons.name == self.season_name).one()

    def club_roster(self, club, comp_season=None):
        """
        Obtain all players who received a contract from a club during a specific competition and season.

        If comp_season is None, calculate clubs in competition/season defined in class.

        :param club: Clubs object
        :param comp_season: CompetitionSeason object of specific competition/season (or None)
        :return: collection of PlayerSalaries objects
        """
        comp_season = comp_season or self.comp_season
        return self.session.query(PlayerSalaries).filter_by(comp_season=comp_season, club=club)

    def number_clubs(self, comp_season=None):
        """
        Returns number of clubs participating in specific season of competition.

        If comp_season is None, calculate clubs in competition/season defined in class.

        :param comp_season: CompetitionSeason object of specific competition/season (or None)
        :return: int (number of teams)
        """
        comp_season = comp_season or self.comp_season
        return self.session.query(LeaguePoints).filter_by(comp_season=comp_season).count()

    def season_payrolls(self, comp_season=None):
        """
        Calculate base salary totals for all MLS clubs participating in competition and season.

        If comp_season is None, calculate clubs in competition/season defined in class.

        :param comp_season: CompetitionSeason object of specific competition/season (or None)
        :return: dictionary of team names and total payroll
        """
        clubs = self.session.query(Clubs).filter(Clubs.symbol.isnot(None)).all()
        payroll_list = []
        for club in clubs:
            team_base = self.club_roster(club, comp_season)
            if team_base.count() > 0:
                payroll_list.append(
                    dict(team=club.name, payroll=sum([player.base_salary for player in team_base]) / 100.00))
        return payroll_list

    def calc_unused_players(self, comp_season=None):
        """
        Calculate number of players on team roster who did not play in competition during season.

        If comp_season is None, calculate clubs in competition/season defined in class.

        :param comp_season: CompetitionSeason object of specific competition/season (or None)
        :return: integer
        """
        comp_season = comp_season or self.comp_season
        roster = self.session.query(PlayerSalaries.player_id).filter_by(comp_season=comp_season).all()
        on_field = self.session.query(CommonStats.player_id).filter(CommonStats.minutes.isnot(None)). \
            filter_by(comp_season=comp_season).all()
        return len(set(roster) - set(on_field))

    def player_weeks(self, club, player_on_roster, comp_season=None):
        """
        Calculate number of weeks that player spent on club roster during competition and season.

        If comp_season is None, calculate clubs in competition/season defined in class.

        :param club: Clubs object
        :param player_on_roster: PlayerSalaries object, player on team roster
        :param comp_season: CompetitionSeasons object or None
        :return: integer
        """
        comp_season = comp_season or self.comp_season
        tenure = self.session.query(PartialTenures.start_week, PartialTenures.end_week). \
            filter(PartialTenures.club == club, PartialTenures.comp_season == comp_season,
                   PartialTenures.player_id == player_on_roster.player_id)
        if tenure.count() == 0:
            tenure = [(1, comp_season.weeks)]
        duration = sum([(end_week - start_week) + 1 for (start_week, end_week) in tenure])
        return duration

    def club_utilization(self, club, comp_season=None):
        """
        Calculate utilization factor of club during competition and season.

        If comp_season is None, calculate clubs in competition/season defined in class.

        :param club: Clubs object
        :param comp_season: CompetitionSeasons object or None
        :return: tuple of (available payroll, numerator, denominator, utilization factor)
        """
        numerator, denominator, available_payroll = 0.0, 0.0, 0.0
        comp_season = comp_season or self.comp_season
        for club_player in self.club_roster(club, comp_season):
            stat_record = self.session.query(CommonStats.minutes).filter_by(
                comp_season=comp_season, club_id=club.id, player_id=club_player.player_id)
            minutes = stat_record.first()[0] if stat_record.first() else 0
            tenure = self.player_weeks(club, club_player, comp_season)
            prorated_salary = float(club_player.base_salary * tenure) / comp_season.weeks
            max_minutes = (float(tenure) / comp_season.weeks) * 90.0 * comp_season.matchdays
            available_payroll += prorated_salary
            numerator += minutes * prorated_salary
            denominator += max_minutes * prorated_salary
        util_factor = numerator / denominator if denominator != 0.0 else 0.0
        return available_payroll, numerator, denominator, util_factor

    def league_utilization(self, comp_season=None):
        """
        Calculate utilization factor of league during competition and season.

        If comp_season is None, calculate clubs in competition/season defined in class.

        :param comp_season: CompetitionSeasons object or None
        :return: tuple of (available payroll, utilization factor)
        """
        numerator, denominator, available_payroll = 0.0, 0.0, 0.0
        clubs = self.session.query(Clubs).filter(Clubs.symbol.isnot(None)).all()
        for club in clubs:
            tp, num, denom, util = self.club_utilization(club, comp_season)
            numerator += num
            denominator += denom
            available_payroll += tp
        league_util = numerator / denominator if denominator != 0.0 else 0.0
        return available_payroll, league_util

    def club_efficiency(self, club, comp_season=None):
        """
        Calculate front-office efficiency rating of a club during a league competition season.

        If comp_season is None, calculate clubs in competition/season defined in class.

        Returns payroll cost per point per game, in cents.

        :param club: Clubs object
        :param comp_season: CompetitionSeasons object or None
        :return: float
        """
        comp_season = comp_season or self.comp_season
        avail_payroll, _, _, util_factor = self.club_utilization(club, comp_season)
        points, = self.session.query(LeaguePoints.points).filter(
            LeaguePoints.comp_season == comp_season, LeaguePoints.club_id == club.id).one()
        points_per_game = float(points) / comp_season.matchdays
        return (avail_payroll * util_factor) / points_per_game

    def league_efficiency(self, comp_season=None):
        """
        Calculate average front-office efficiency of league competition season.

        If comp_season is None, calculate clubs in competition/season defined in class.

        Returns payroll cost per point per game, in cents.

        :param comp_season: CompetitionSeasons object for competition/season or None
        :return: float
        """
        comp_season = comp_season or self.comp_season
        avail_payroll, util_factor = self.league_utilization(comp_season)
        points = self.session.query(func.sum(LeaguePoints.points)).filter(
            LeaguePoints.comp_season == comp_season).scalar()
        matches_played = self.session.query(func.sum(LeaguePoints.played)).filter(
            LeaguePoints.comp_season == comp_season).scalar()
        number_clubs = self.number_clubs(comp_season)
        points_per_game = float(points) / matches_played
        return (avail_payroll / number_clubs * util_factor) / points_per_game

    def inflation_factor(self, comp_season_base):
        """
        Calculate inflation factor, which is the baseline average payroll per club divided by the average
        payroll per club in the current season.

        The "current" season is the combination of competition/season defined at class initialization.

        If the baseline season is the current one, return 1.0.

        :param comp_season_base: CompetitionSeasons object for baseline competition/season
        :return: float
        """
        if comp_season_base == self.comp_season:
            return 1.0
        else:
            avail_payroll_current, _ = self.league_utilization()
            avg_avail_payroll_current = float(avail_payroll_current)/self.number_clubs()
            avail_payroll_base, _ = self.league_utilization(comp_season_base)
            avg_avail_payroll_base = float(avail_payroll_base)/self.number_clubs(comp_season_base)
            return avg_avail_payroll_base / avg_avail_payroll_current

    def win_cost(self, club, comp_season_base):
        """
        Calculate the standard win cost of a club in a competition season, relative to a baseline season.

        :param club: Clubs object
        :param comp_season_base: CompetitionSeasons object for baseline competition/season
        :return: float
        """
        return (self.club_efficiency(club) *
                self.inflation_factor(comp_season_base) / self.league_efficiency(comp_season_base) * 100.0)
