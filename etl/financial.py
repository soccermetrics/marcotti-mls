from models import (Countries, Players, PlayerSalaries, PartialTenures, AcquisitionPaths,
                    AcquisitionType, PlayerDrafts, Clubs, Competitions, Seasons, Years)
from etl import BaseCSV, PersonIngest


class AcquisitionIngest(PersonIngest):

    def parse_file(self, rows):
        insertion_list = []
        print "Ingesting Player Acquisition Paths..."
        for keys in rows:
            player_tuple = self.get_person_data(**keys)
            country_name = self.column_unicode("Country", **keys)
            path = self.column("Acquisition", **keys)
            acquisition_year = self.column("Year", **keys)

            acquisition_path = AcquisitionType.from_string(path)
            country_id = self.get_id(Countries, name=country_name)
            year_id = self.get_id(Years, name=acquisition_year)

            player_tuple = dict(country_id=country_id, **player_tuple)
            player_id = self.get_id(Players, **player_tuple)

            acquisition_tuple = dict(player_id=player_id, year_id=year_id)
            if not self.record_exists(AcquisitionPaths, **acquisition_tuple):
                acquisition_record = AcquisitionPaths(**acquisition_tuple)
                if acquisition_path in [AcquisitionType.college_draft, AcquisitionType.inaugural_draft,
                                        AcquisitionType.super_draft, AcquisitionType.supplemental_draft]:
                    acquisition_record = self.parse_draft_data(acquisition_tuple, keys)
                insertion_list.append(acquisition_record)
                if len(insertion_list) == 50:
                    self.session.add_all(insertion_list)
                    self.session.commit()
                    insertion_list = []
        if len(insertion_list) != 0:
            self.session.add_all(insertion_list)
        print "Acquisition Ingestion complete."

    def parse_draft_data(self, acq_tuple, keys):
        draft_round = self.column_int("Round", **keys)
        draft_selection = self.column_int("Pick", **keys)
        return PlayerDrafts(round=draft_round, selection=draft_selection, **acq_tuple)


class PlayerSalaryIngest(BaseCSV):

    def __init__(self, session, competition, season):
        super(PlayerSalaryIngest, self).__init__(session)
        self.competition_id = self.get_id(Competitions, name=competition)
        self.season_id = self.get_id(Seasons, name=season)

    def parse_file(self, rows):
        insertion_list = []
        print "Ingesting Player Salaries..."
        for keys in rows:
            club_symbol = self.column("Team", **keys)
            last_name = self.column_unicode("Last Name", **keys)
            first_name = self.column_unicode("First Name", **keys)
            base_salary = int(self.column_float("Base", **keys) * 100)
            supp_salary = int(self.column_float("Supp", **keys) * 100)

            full_name = " ".join([first_name, last_name])
            club_id = self.get_id(Clubs, symbol=club_symbol)
            player_id = self.get_id(Players, full_name=full_name)

            salary_tuple = dict(player_id=player_id, club_id=club_id,
                                competition_id=self.competition_id,
                                season_id=self.season_id)
            if not self.record_exists(PlayerSalaries, **salary_tuple):
                insertion_list.append(PlayerSalaries(base_salary=base_salary,
                                                     avg_guaranteed=supp_salary,
                                                     **salary_tuple))
                if len(insertion_list) == 50:
                    self.session.add_all(insertion_list)
                    self.session.commit()
                    insertion_list = []
        if len(insertion_list) != 0:
            self.session.add_all(insertion_list)
        print "Salary Ingestion complete."


class PartialTenureIngest(BaseCSV):

    def __init__(self, session, competition, season):
        super(PartialTenureIngest, self).__init__(session)
        self.competition_id = self.get_id(Competitions, name=competition)
        self.season_id = self.get_id(Seasons, name=season)

    def parse_file(self, rows):
        insertion_list = []
        print "Ingesting Partial Tenure records..."
        for keys in rows:
            club_symbol = self.column("Team", **keys)
            last_name = self.column_unicode("Last Name", **keys)
            first_name = self.column_unicode("First Name", **keys)
            start_week = self.column_int("Start Term", **keys)
            end_week = self.column_int("End Term", **keys)

            full_name = " ".join([first_name, last_name])
            club_id = self.get_id(Clubs, symbol=club_symbol)
            player_id = self.get_id(Players, full_name=full_name)

            partial_tuple = dict(player_id=player_id, club_id=club_id,
                                 competition_id=self.competition_id,
                                 season_id=self.season_id)
            if not self.record_exists(PartialTenures, **partial_tuple):
                insertion_list.append(PartialTenures(start_week=start_week,
                                                     end_week=end_week,
                                                     **partial_tuple))
                if len(insertion_list) == 50:
                    self.session.add_all(insertion_list)
                    self.session.commit()
                    insertion_list = []
        if len(insertion_list) != 0:
            self.session.add_all(insertion_list)
        print "Partial Tenure Ingestion complete."
