import logging

from models import (Countries, Players, PlayerSalaries, PartialTenures, AcquisitionPaths,
                    AcquisitionType, PlayerDrafts, Competitions, Clubs, Years, Seasons)
from etl import PersonIngest, SeasonalDataIngest


logger = logging.getLogger(__name__)


class AcquisitionIngest(PersonIngest):

    BATCH_SIZE = 200

    def parse_file(self, rows):
        inserts = 0
        insertion_list = []
        logger.info("Ingesting Player Acquisition Paths...")
        for keys in rows:
            person_dict = self.get_person_data(**keys)
            country_name = self.column_unicode("Country", **keys)
            path = self.column("Acquisition", **keys)
            acquisition_year = self.column("Year", **keys)

            try:
                acquisition_path = AcquisitionType.from_string(path)
            except ValueError:
                acquisition_path = None
            country_id = self.get_id(Countries, name=country_name)
            if country_id is None:
                logger.error(u"Cannot insert Acquisition record for {}: "
                             u"Database error involving Country record {}".format(person_dict, country_name))
                continue
            year_id = self.get_id(Years, yr=acquisition_year)
            if year_id is None:
                logger.error(u"Cannot insert Acquisition record for {}: "
                             u"Database error involving Year record {}".format(person_dict, acquisition_year))
                continue
            player_dict = dict(country_id=country_id, **person_dict)
            player_id = self.get_id(Players, **player_dict)
            if player_id is None:
                logger.error(u"Cannot insert Acquisition record for {}: "
                             u"Database error involving Player record".format(player_dict))
                continue

            acquisition_dict = dict(player_id=player_id, year_id=year_id, path=acquisition_path)
            if not self.record_exists(AcquisitionPaths, **acquisition_dict):
                acquisition_record = AcquisitionPaths(**acquisition_dict)
                if acquisition_path in [AcquisitionType.college_draft, AcquisitionType.inaugural_draft,
                                        AcquisitionType.super_draft, AcquisitionType.supplemental_draft]:
                    acquisition_record = self.parse_draft_data(acquisition_dict, keys)
                if acquisition_record is not None:
                    insertion_list.append(acquisition_record)
                inserted, insertion_list = self.bulk_insert(insertion_list, AcquisitionIngest.BATCH_SIZE)
                inserts += inserted
                if inserted and not inserts % AcquisitionIngest.BATCH_SIZE:
                    logger.info("{} records inserted".format(inserts))
        self.session.add_all(insertion_list)
        self.session.commit()
        inserts += len(insertion_list)
        logger.info("Total {} Acquisition records inserted and committed to database".format(inserts))
        logger.info("Acquisition Ingestion complete.")

    def parse_draft_data(self, acq_tuple, keys):
        draft_round = self.column_int("Round", **keys)
        draft_selection = self.column_int("Pick", **keys)
        is_generation_adidas = self.column_bool("Gen Adidas", **keys)
        drafting_club = self.column_unicode("Acquiring Club", **keys)

        club_id = self.get_id(Clubs, name=drafting_club)
        if club_id is None:
            logger.error(u"Cannot insert {p[Acquisition]} record for {p[First Name]} {p[Last Name]}: "
                         u"Club {p[Acquiring Club]} not in database".format(p=keys))
            return None
        return PlayerDrafts(round=draft_round, selection=draft_selection,
                            gen_adidas=is_generation_adidas, club_id=club_id, **acq_tuple)


class PlayerSalaryIngest(SeasonalDataIngest):

    BATCH_SIZE = 100

    def parse_file(self, rows):
        inserts = 0
        insertion_list = []
        logger.info("Ingesting Player Salaries...")
        for keys in rows:
            competition_name = self.column_unicode("Competition", **keys)
            season_name = self.column("Season", **keys)
            club_symbol = self.column("Club Symbol", **keys)
            last_name = self.column_unicode("Last Name", **keys)
            first_name = self.column_unicode("First Name", **keys)
            base_salary = int(self.column_float("Base", **keys) * 100)
            guar_salary = int(self.column_float("Guaranteed", **keys) * 100)

            competition_id = self.get_id(Competitions, name=competition_name)
            if competition_id is None:
                logger.error(u"Cannot insert Salary record for {} {}: "
                             u"Competition {} not in database".format(first_name, last_name, competition_name))
                continue
            season_id = self.get_id(Seasons, name=season_name)
            if season_id is None:
                logger.error(u"Cannot insert Salary record for {} {}: "
                             u"Season {} not in database".format(first_name, last_name, season_name))
                continue
            club_id = self.get_id(Clubs, symbol=club_symbol)
            if club_id is None:
                logger.error(u"Cannot insert Salary record for {} {}: "
                             u"Club {} not in database".format(first_name, last_name, club_symbol))
                continue
            player_id = self.get_player_from_name(first_name, last_name)
            if player_id is None:
                logger.error(u"Cannot insert Salary record for {} {}: "
                             u"Player not in database".format(first_name, last_name))
                continue

            salary_dict = dict(player_id=player_id, club_id=club_id,
                               competition_id=competition_id, season_id=season_id)
            if not self.record_exists(PlayerSalaries, **salary_dict):
                insertion_list.append(PlayerSalaries(base_salary=base_salary,
                                                     avg_guaranteed=guar_salary,
                                                     **salary_dict))
                inserted, insertion_list = self.bulk_insert(insertion_list, PlayerSalaryIngest.BATCH_SIZE)
                inserts += inserted
                if inserted and not inserts % PlayerSalaryIngest.BATCH_SIZE:
                    logger.info("{} records inserted".format(inserts))
        self.session.add_all(insertion_list)
        self.session.commit()
        inserts += len(insertion_list)
        logger.info("Total {} Player Salary records inserted and committed to database".format(inserts))
        logger.info("Player Salary Ingestion complete.")


class PartialTenureIngest(SeasonalDataIngest):

    BATCH_SIZE = 10

    def parse_file(self, rows):
        inserts = 0
        insertion_list = []
        logger.info("Ingesting Partial Tenure records...")
        for keys in rows:
            competition_name = self.column_unicode("Competition", **keys)
            season_name = self.column("Season", **keys)
            club_symbol = self.column("Club Symbol", **keys)
            last_name = self.column_unicode("Last Name", **keys)
            first_name = self.column_unicode("First Name", **keys)
            start_week = self.column_int("Start Term", **keys)
            end_week = self.column_int("End Term", **keys)

            competition_id = self.get_id(Competitions, name=competition_name)
            if competition_id is None:
                logger.error(u"Cannot insert Partial Tenure record for {} {}: "
                             u"Competition {} not in database".format(first_name, last_name, competition_name))
                continue
            season_id = self.get_id(Seasons, name=season_name)
            if season_id is None:
                logger.error(u"Cannot insert Partial Tenure record for {} {}: "
                             u"Season {} not in database".format(first_name, last_name, season_name))
                continue
            club_id = self.get_id(Clubs, symbol=club_symbol)
            if club_id is None:
                logger.error(u"Cannot insert Partial Tenure record for {} {}: "
                             u"Club {} not in database".format(first_name, last_name, club_symbol))
                continue
            player_id = self.get_player_from_name(first_name, last_name)
            if player_id is None:
                logger.error(u"Cannot insert Partial Tenure record for {} {}: "
                             u"Player not in database".format(first_name, last_name))
                continue

            partials_dict = dict(player_id=player_id, club_id=club_id,
                                 competition_id=competition_id,
                                 season_id=season_id)
            if not self.record_exists(PartialTenures, **partials_dict):
                insertion_list.append(PartialTenures(start_week=start_week,
                                                     end_week=end_week,
                                                     **partials_dict))
                inserted, insertion_list = self.bulk_insert(insertion_list, PartialTenureIngest.BATCH_SIZE)
                inserts += inserted
        self.session.add_all(insertion_list)
        self.session.commit()
        inserts += len(insertion_list)
        logger.info("Total {} Partial Tenure records inserted and committed to database".format(inserts))
        logger.info("Partial Tenure Ingestion complete.")
