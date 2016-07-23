import logging

from etl.base import SeasonalDataIngest
from models import *


logger = logging.getLogger(__name__)


class PlayerMinuteIngest(SeasonalDataIngest):
    """
    Ingestion methods for data files containing player minutes.
    """
    
    def parse_file(self, rows):
        inserts = 0
        insertion_list = []
        logger.info("Ingesting Player Minutes...")
        for keys in rows:
            competition_name = self.column_unicode("Competition", **keys)
            season_name = self.column("Season", **keys)
            club_symbol = self.column("Club Symbol", **keys)
            last_name = self.column_unicode("Last Name", **keys)
            first_name = self.column_unicode("First Name", **keys)
            total_minutes = self.column_int("Mins", **keys)

            competition_id = self.get_id(Competitions, name=competition_name)
            if competition_id is None:
                logger.error(u"Cannot insert Player Minutes record for {} {}: "
                             u"Competition {} not in database".format(first_name, last_name, competition_name))
                continue
            season_id = self.get_id(Seasons, name=season_name)
            if season_id is None:
                logger.error(u"Cannot insert Player Minutes record for {} {}: "
                             u"Season {} not in database".format(first_name, last_name, season_name))
                continue
            club_id = self.get_id(Clubs, symbol=club_symbol)
            if club_id is None:
                logger.error(u"Cannot insert Player Minutes record for {} {}: "
                             u"Club {} not in database".format(first_name, last_name, club_symbol))
                continue
            player_id = self.get_player_from_name(first_name, last_name)
            if player_id is None:
                logger.error(u"Cannot insert Player Minutes record for {} {}: "
                             u"Player not in database".format(first_name, last_name))
                continue

            stat_dict = self.prepare_db_dict(
                ['player_id', 'club_id', 'competition_id', 'season_id', 'minutes'],
                [player_id, club_id, competition_id, season_id, total_minutes])
            if not self.record_exists(FieldPlayerStats, **stat_dict):
                insertion_list.append(FieldPlayerStats(**stat_dict))
                inserted, insertion_list = self.bulk_insert(insertion_list, 50)
                inserts += inserted
        self.session.add_all(insertion_list)
        self.session.commit()
        inserts += len(insertion_list)
        logger.info("Total {} Player Minutes records inserted and committed to database".format(inserts))
        logger.info("Player Minutes Ingestion complete.")


class MatchStatIngest(SeasonalDataIngest):
    """
    Ingestion methods for data files containing season statistics.
    
    Assume categories and nomenclature of Nielsen soccer database.
    """

    @staticmethod
    def is_empty_record(*args):
        """Check for sparseness of statistical record.

        If all quantities of a statistical record are zero, return True.

        If at least one quantity of statistical record is nonzero, return False.
        """
        return not any([arg for arg in args])

    def get_common_stats(self, **keys):
        last_name = self.column_unicode("Last Name", **keys)
        first_name = self.column_unicode("First Name", **keys)
        club_name = self.column_unicode("Club", **keys)
        competition_name = self.column_unicode("Competition", **keys)
        start_year = self.column_int("Year1", **keys)
        end_year = self.column_int("Year2", **keys)
        match_appearances = self.column_int("Gp", **keys)
        matches_subbed = self.column_int("Sb", **keys)
        total_minutes = self.column_int("Min", **keys)
        yellow_cards = self.column_int("Yc", **keys)
        red_cards = self.column_int("Rc", **keys)

        player_id = self.get_player_from_name(first_name, last_name)
        club_id = self.get_id(Clubs, name=club_name)
        competition_id = self.get_id(Competitions, name=competition_name)
        season_name = "{}".format(start_year) if start_year == end_year else "{}-{}".format(start_year, end_year)
        season_id = self.get_id(Seasons, name=season_name)
        
        stat_dict = self.prepare_db_dict(
            ['player_id', 'club_id', 'competition_id', 'season_id', 'appearances', 
             'substituted', 'minutes', 'yellows', 'reds'], 
            [player_id, club_id, competition_id, season_id, match_appearances,
             matches_subbed, total_minutes, yellow_cards, red_cards])
        return stat_dict

    def parse_file(self, rows):
        raise NotImplementedError


class FieldStatIngest(MatchStatIngest):
    
    def parse_file(self, rows):
        inserts = 0
        insertion_list = []
        for keys in rows:
            common_stat_dict = self.get_common_stats(**keys)

            total_goals = self.column_int("Gl", **keys)
            headed_goals = self.column_int("Hd", **keys)
            freekick_goals = self.column_int("Fk", **keys)
            in_box_goals = self.column_int("In", **keys)
            out_box_goals = self.column_int("Out", **keys)
            game_winning_goals = self.column_int("Gw", **keys)
            penalty_goals = self.column_int("Pn", **keys)
            total_penalties = self.column_int("Pa", **keys)
            assists = self.column_int("As", **keys)
            deadball_assists = self.column_int("Dd", **keys)
            shots = self.column_int("Sht", **keys)
            fouls = self.column_int("Fls", **keys)

            field_stat_dict = self.prepare_db_dict(
                ['goals_total', 'goals_headed', 'goals_freekick', 'goals_in_area', 'goals_out_area',
                 'goals_winners', 'goals_penalty', 'penalties_taken', 'assists_total', 'assists_deadball',
                 'shots_total', 'fouls_total'],
                [total_goals, headed_goals, freekick_goals, in_box_goals, out_box_goals,
                 game_winning_goals, penalty_goals, total_penalties, assists, deadball_assists,
                 shots, fouls]
            )
            field_stat_dict.update(common_stat_dict)

            if field_stat_dict is not None:
                if not self.record_exists(FieldPlayerStats, **field_stat_dict):
                    insertion_list.append(FieldPlayerStats(**field_stat_dict))
                    inserted, insertion_list = self.bulk_insert(insertion_list, 50)
                    inserts += inserted
        self.session.add_all(insertion_list)
        self.session.commit()
        inserts += len(insertion_list)
        logger.info("Total {} Field Player Statistics records inserted and committed to database".format(inserts))
        logger.info("Field Player Statistics Ingestion complete.")


class GoalkeeperStatIngest(MatchStatIngest):

    def parse_file(self, rows):
        inserts = 0
        insertion_list = []
        for keys in rows:
            common_stat_dict = self.get_common_stats(**keys)

            wins = self.column_int("Wn", **keys)
            draws = self.column_int("Dr", **keys)
            losses = self.column_int("Ls", **keys)
            goals_allowed = self.column_int("Ga", **keys)
            clean_sheets = self.column_int("Cs", **keys)
            shots_allowed = self.column_int("Sht", **keys)

            gk_stat_dict = self.prepare_db_dict(
                ['wins', 'draws', 'losses', 'goals_allowed', 'shots_allowed', 'clean_sheets'],
                [wins, draws, losses, goals_allowed, clean_sheets, shots_allowed]
            )
            gk_stat_dict.update(common_stat_dict)

            if gk_stat_dict is not None:
                if not self.record_exists(GoalkeeperStats, **gk_stat_dict):
                    stat_record = GoalkeeperStats(**gk_stat_dict)
                    insertion_list.append(stat_record)
                    inserted, insertion_list = self.bulk_insert(insertion_list, 50)
                    inserts += inserted 
        self.session.add_all(insertion_list)
        self.session.commit()
        inserts += len(insertion_list)
        logger.info("Total {} Goalkeeper Statistics records inserted and committed to database".format(inserts))
        logger.info("Goalkeeper Statistics Ingestion complete.")


class LeaguePointIngest(SeasonalDataIngest):

    def parse_file(self, rows):
        inserts = 0
        insertion_list = []
        for keys in rows:
            try:
                club_symbol = self.column("Club Symbol", **keys)
            except KeyError:
                club_symbol = None
            try:
                club_name = self.column_unicode("Club", **keys)
            except KeyError:
                club_name = None
            competition_name = self.column_unicode("Competition", **keys)
            season_name = self.column("Season", **keys)
            matches_played = self.column_int("GP", **keys)
            points = self.column_int("Pts", **keys)

            competition_id = self.get_id(Competitions, name=competition_name)
            if competition_id is None:
                logger.error(u"Cannot insert LeaguePoint record: "
                             u"Competition {} not in database".format(competition_name))
                continue
            season_id = self.get_id(Seasons, name=season_name)
            if season_id is None:
                logger.error(u"Cannot insert LeaguePoint record: "
                             u"Season {} not in database".format(season_name))
                continue
            club_dict = {field: value for (field, value)
                         in zip(['name', 'symbol'], [club_name, club_symbol])
                         if value is not None}
            club_id = self.get_id(Clubs, **club_dict)
            if club_id is None:
                logger.error(u"Cannot insert LeaguePoint record: "
                             u"Database error involving {}".format(club_dict))
                continue

            club_season_dict = dict(club_id=club_id, competition_id=competition_id, season_id=season_id)
            if not self.record_exists(LeaguePoints, **club_season_dict):
                point_record_dict = dict(played=matches_played, points=points)
                point_record_dict.update(club_season_dict)
                insertion_list.append(LeaguePoints(**point_record_dict))
                inserted, insertion_list = self.bulk_insert(insertion_list, 10)
                inserts += inserted
        self.session.add_all(insertion_list)
        self.session.commit()
        inserts += len(insertion_list)
        logger.info("Total {} League Point records inserted and committed to database".format(inserts))
        logger.info("League Point Ingestion complete.")
