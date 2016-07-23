from sqlalchemy.sql import func

from models import *


class Analytics(object):
    """
    Class that contains methods that carries out analysis on the financial and draft data.
    """

    def __init__(self, session):
        self.session = session

    def get_team_roster(self, comp_season, club):
        """
        Obtain all players who received a contract from a team during a specific competition and season.

        :param comp_season: CompetitionSeasons object
        :param club: Clubs object
        :return: collection of PlayerSalaries objects
        """
        roster = self.session.query(PlayerSalaries).filter_by(comp_season=comp_season, club=club)
        return roster

    def calc_number_teams_season(self, comp_season):
        """
        Calculate number of teams participating in specific season of competition.

        :param comp_season: CompetitionSeason object
        :return: int
        """
        return self.session.query(LeaguePoints).filter_by(comp_season=comp_season).count()
    
    def calc_season_payrolls(self, comp_season):
        """
        Calculate base salary totals for all clubs participating in competition and season.

        :param comp_season: CompetitionSeason object
        :return: dictionary of team names and total payroll
        """
        clubs = self.session.query(Clubs).filter(Clubs.symbol.isnot(None)).all()
        payroll_list = []
        for club in clubs:
            team_base = self.get_team_roster(comp_season, club)
            if team_base.count() > 0:
                payroll_list.append(
                    dict(team=club.name, payroll=sum([player.base_salary for player in team_base]) / 100.00))
        return payroll_list

    def calc_player_tenure(self, comp_season, club, player_on_roster):
        """
        Calculate number of weeks that player spent on club roster during competition and season.

        :param comp_season: CompetitionSeasons object
        :param club: Clubs object
        :param player_on_roster: PlayerSalaries object, player on team roster
        :return: integer
        """
        tenure = self.session.query(PartialTenures.start_week, PartialTenures.end_week). \
            filter(PartialTenures.club == club, PartialTenures.comp_season == comp_season,
                   PartialTenures.player_id == player_on_roster.player_id)
        if tenure.count() == 0:
            tenure = [(1, comp_season.weeks)]
        duration = sum([(end_week - start_week) + 1 for (start_week, end_week) in tenure])
        return duration

    def calc_unused_players(self, comp_season):
        """
        Calculate number of players on team roster who did not play in competition during season.
        :param comp_season: CompetitionSeasons object
        :return: integer
        """
        roster = self.session.query(PlayerSalaries.player_id).filter_by(comp_season=comp_season).all()
        on_field = self.session.query(CommonStats.player_id).filter(CommonStats.minutes.isnot(None)). \
            filter_by(comp_season=comp_season).all()
        return len(set(roster) - set(on_field))

    def calc_club_utilization(self, comp_season, club):
        """
        Calculate utilization factor of club during competition and season.

        :param comp_season: CompetitionSeasons object
        :param club: Clubs object
        :return: tuple of (available payroll, numerator, denominator, utilization factor)
        """
        numerator, denominator, available_payroll = 0.0, 0.0, 0.0
        for club_player in self.get_team_roster(comp_season, club):
            stat_record = self.session.query(CommonStats.minutes).filter_by(
                comp_season=comp_season, club_id=club.id, player_id=club_player.player_id)
            minutes = stat_record.first()[0] if stat_record.first() else 0
            tenure = self.calc_player_tenure(comp_season, club, club_player)
            prorated_salary = float(club_player.base_salary * tenure) / comp_season.weeks
            max_minutes = (float(tenure) / comp_season.weeks) * 90.0 * comp_season.matchdays
            available_payroll += prorated_salary
            numerator += minutes * prorated_salary
            denominator += max_minutes * prorated_salary
        util_factor = numerator / denominator if denominator != 0.0 else 0.0
        return available_payroll, numerator, denominator, util_factor

    def calc_league_utilization(self, comp_season):
        """
        Calculate utilization factor of league during competition and season.

        :param comp_season: CompetitionSeasons object
        :return: tuple of (available payroll, utilization factor)
        """
        numerator, denominator, available_payroll = 0.0, 0.0, 0.0
        clubs = self.session.query(Clubs).filter(Clubs.symbol.isnot(None)).all()
        for club in clubs:
            tp, num, denom, util = self.calc_club_utilization(comp_season, club)
            numerator += num
            denominator += denom
            available_payroll += tp
        league_util = numerator / denominator if denominator != 0.0 else 0.0
        return available_payroll, league_util

    def calc_club_efficiency(self, comp_season, club):
        """
        Calculate front-office efficiency rating of a club during a league competition season.

        Returns payroll cost per point per game, in cents.

        :param comp_season: CompetitionSeasons object
        :param club: Clubs object
        :return: float
        """
        avail_payroll, _, _, util_factor = self.calc_club_utilization(comp_season, club)
        points = self.session.query(LeaguePoints.points).filter(
            LeaguePoints.comp_season == comp_season, LeaguePoints.club_id == club.id).one()
        points_per_game = float(points) / comp_season.matchdays
        return (avail_payroll * util_factor) / points_per_game

    def calc_league_efficiency(self, comp_season):
        """
        Calculate average front-office efficiency of league competition season.

        Returns payroll cost per point per game, in cents.

        :param comp_season: CompetitionSeasons object
        :return: float
        """
        avail_payroll, util_factor = self.calc_league_utilization(comp_season)
        points = self.session.query(func.sum(LeaguePoints.points)).filter(
            LeaguePoints.comp_season == comp_season).scalar()
        matches_played = self.session.query(func.sum(LeaguePoints.played)).filter(
            LeaguePoints.comp_season == comp_season).scalar()
        number_clubs = self.calc_number_teams_season(comp_season)
        points_per_game = float(points) / matches_played
        return (avail_payroll / number_clubs * util_factor) / points_per_game

    def calc_inflation_factor(self, comp_season_current, comp_season_base):
        """
        Calculate inflation factor, which is the baseline average payroll per club divided by the average
        payroll per club in the current season.

        If the baseline season is the current one, return 1.0.

        :param comp_season_current: CompetitionSeasons object for current competition/season
        :param comp_season_base: CompetitionSeasons object for baseline competition/season
        :return: float
        """
        if comp_season_current == comp_season_base:
            return 1.0
        else:
            avail_payroll_current, _ = self.calc_league_utilization(comp_season_current)
            avg_avail_payroll_current = float(avail_payroll_current)/self.calc_number_teams_season(comp_season_current)
            avail_payroll_base, _ = self.calc_league_utilization(comp_season_base)
            avg_avail_payroll_base = float(avail_payroll_base)/self.calc_number_teams_season(comp_season_base)
            return avg_avail_payroll_base / avg_avail_payroll_current

    def calc_win_cost(self, club, comp_season, comp_season_base):
        """
        Calculate the standard win cost of a club in a competition season, relative to a baseline season.

        :param club: Clubs object
        :param comp_season: CompetitionSeasons object for current competition/season
        :param comp_season_base: CompetitionSeasons object for baseline competition/season
        :return: float
        """
        return (self.calc_club_efficiency(comp_season, club) *
                self.calc_inflation_factor(comp_season, comp_season_base) /
                self.calc_league_efficiency(comp_season_base) * 100.0)