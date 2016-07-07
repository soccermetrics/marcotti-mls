import logging

from models import (Countries, Players, PlayerSalaries, PartialTenures, AcquisitionPaths,
                    AcquisitionType, PlayerDrafts, Clubs, Years)
from etl import PersonIngest, SeasonalDataIngest


logger = logging.getLogger(__name__)


class AcquisitionIngest(PersonIngest):

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
                insertion_list.append(acquisition_record)
                inserted, insertion_list = self.bulk_insert(insertion_list, 200)
                inserts += inserted
        self.session.add_all(insertion_list)
        self.session.commit()
        inserts += len(insertion_list)
        logger.info("Total {} Acquisition records inserted and committed to database".format(inserts))
        logger.info("Acquisition Ingestion complete.")

    def parse_draft_data(self, acq_tuple, keys):
        draft_round = self.column_int("Round", **keys)
        draft_selection = self.column_int("Pick", **keys)
        return PlayerDrafts(round=draft_round, selection=draft_selection, **acq_tuple)


class PlayerSalaryIngest(SeasonalDataIngest):

    def parse_file(self, rows):
        inserts = 0
        insertion_list = []
        logger.info("Ingesting Player Salaries...")
        for keys in rows:
            club_symbol = self.column("Team Symbol", **keys)
            last_name = self.column_unicode("Last Name", **keys)
            first_name = self.column_unicode("First Name", **keys)
            base_salary = int(self.column_float("Base", **keys) * 100)
            guar_salary = int(self.column_float("Guaranteed", **keys) * 100)

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
                               competition_id=self.competition_id, season_id=self.season_id)
            if not self.record_exists(PlayerSalaries, **salary_dict):
                insertion_list.append(PlayerSalaries(base_salary=base_salary,
                                                     avg_guaranteed=guar_salary,
                                                     **salary_dict))
                inserted, insertion_list = self.bulk_insert(insertion_list, 100)
                inserts += inserted
        self.session.add_all(insertion_list)
        self.session.commit()
        inserts += len(insertion_list)
        logger.info("Total {} Player Salary records inserted and committed to database".format(inserts))
        logger.info("Player Salary Ingestion complete.")


class PartialTenureIngest(SeasonalDataIngest):

    def parse_file(self, rows):
        inserts = 0
        insertion_list = []
        logger.info("Ingesting Partial Tenure records...")
        for keys in rows:
            club_symbol = self.column("Team Symbol", **keys)
            last_name = self.column_unicode("Last Name", **keys)
            first_name = self.column_unicode("First Name", **keys)
            start_week = self.column_int("Start Term", **keys)
            end_week = self.column_int("End Term", **keys)

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
                                 competition_id=self.competition_id,
                                 season_id=self.season_id)
            if not self.record_exists(PartialTenures, **partials_dict):
                insertion_list.append(PartialTenures(start_week=start_week,
                                                     end_week=end_week,
                                                     **partials_dict))
                inserted, insertion_list = self.bulk_insert(insertion_list, 10)
                inserts += inserted
        self.session.add_all(insertion_list)
        self.session.commit()
        inserts += len(insertion_list)
        logger.info("Total {} Partial Tenure records inserted and committed to database".format(inserts))
        logger.info("Partial Tenure Ingestion complete.")
