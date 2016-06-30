import re
from datetime import date

from sqlalchemy.orm.exc import NoResultFound

from models import (Countries, Clubs, DomesticCompetitions, InternationalCompetitions,
                    Persons, Players, NameOrderType, PositionType, ConfederationType)
from etl.base import BaseCSV


class CountryIngest(BaseCSV):

    def parse_file(self, rows):
        insertion_list = []
        print "Ingesting Countries..."
        for keys in rows:
            country_name = "{name}".format(**keys).strip().decode('utf-8')
            confederation = "{confederation}".format(**keys).strip()
            if not self.record_exists(Countries, name=country_name):
                insertion_list.append(Countries(
                        name=country_name, confederation=ConfederationType.from_string(confederation)))
                if len(insertion_list) == 50:
                    self.session.add_all(insertion_list)
                    self.session.commit()
                    insertion_list = []
        if len(insertion_list) != 0:
            self.session.add_all(insertion_list)
        print "Country Ingestion complete."


class CompetitionIngest(BaseCSV):

    def parse_file(self, rows):
        print "Ingesting Competitions..."
        for keys in rows:
            name = self.column_unicode("Name", **keys)
            level = self.column_int("Level", **keys)
            country_name = self.column_unicode("Country", **keys)
            confederation_name = self.column_unicode("Confederation", **keys)

            if all(var is not None for var in [country_name, confederation_name]):
                print "Cannot insert Competition record: Country and Confederation defined"
                continue
            else:
                comp_record = None
                if country_name is not None:
                    country_id = self.get_id(Countries, name=country_name)
                    comp_dict = dict(name=name, level=level, country_id=country_id)
                    if country_id is not None and not self.record_exists(DomesticCompetitions, **comp_dict):
                        comp_record = DomesticCompetitions(**comp_dict)
                    else:
                        print "Cannot insert Competition record: Country not found or Record exists"
                        continue
                elif confederation_name is not None:
                    try:
                        confederation = ConfederationType.from_string(confederation_name)
                        comp_dict = dict(name=name, level=level, confederation=confederation)
                        if not self.record_exists(InternationalCompetitions, **comp_dict):
                            comp_record = InternationalCompetitions(**comp_dict)
                    except ValueError:
                        print "Cannot insert Competition record: Confederation not found or Record exists"
                        continue
                else:
                    print "Cannot insert Competition record: Neither Country nor Confederation defined"
                    continue
                if comp_record is not None:
                    self.session.add(comp_record)
        self.session.commit()
        print "Competition Ingestion complete."


class ClubIngest(BaseCSV):

    def parse_file(self, rows):
        print "Ingesting Clubs..."
        for keys in rows:
            name = self.column_unicode("Name", **keys)
            symbol = self.column("Symbol", **keys)
            country_name = self.column_unicode("Country", **keys)

            if country_name is None:
                print "Cannot insert Club record: Country required"
                continue
            else:
                country_id = self.get_id(Countries, name=country_name)
                club_dict = dict(name=name, symbol=symbol, country_id=country_id)
                if country_id is not None and not self.record_exists(Clubs, **club_dict):
                    club_record = Clubs(**club_dict)
                    self.session.add(club_record)
        self.session.commit()
        print "Club Ingestion complete."


class PersonIngest(BaseCSV):

    def parse_file(self, rows):
        raise NotImplementedError

    def get_person_data(self, **keys):
        first_name = self.column_unicode("First Name", **keys)
        known_first_name = self.column_unicode("Known First Name", **keys)
        middle_name = self.column_unicode("Middle Name", **keys)
        last_name = self.column_unicode("Last Name", **keys)
        second_last_name = self.column_unicode("Second Last Name", **keys)
        nickname = self.column_unicode("Nickname", **keys)
        date_of_birth = self.column("DOB", **keys)
        order = self.column("Name Order", **keys) or "Western"

        name_order = NameOrderType.from_string(order)
        birth_date = date(*tuple(int(x) for x in date_of_birth.split('-')))

        person_dict = {field: value for (field, value) in zip(
            ['first_name', 'known_first_name', 'middle_name', 'last_name',
             'second_last_name', 'nick_name', 'birth_date', 'order'],
            [first_name, known_first_name, middle_name, last_name,
             second_last_name, nickname, birth_date, name_order]) if value is not None}
        return person_dict


class PlayerIngest(PersonIngest):

    def parse_file(self, rows):
        inserts = 0
        print "Ingesting Players..."
        for keys in rows:
            person_dict = self.get_person_data(**keys)
            position_chars = self.column("Position", **keys)
            country_name = self.column_unicode("Country", **keys)

            country_id = self.get_id(Countries, name=country_name)

            position = [None, None]
            if position_chars is not None:
                position_codes = re.split(r'[;,:\-/]', position_chars)
                for i, code in enumerate(position_codes):
                    position[i] = PositionType.from_string(code)

            person_dict = dict(country_id=country_id, **person_dict)
            if not self.record_exists(Players, **person_dict):
                try:
                    person_id = self.get_id(Persons, **person_dict)
                    player_record = Players(person_id=person_id, primary_position=position[0],
                                            secondary_position=position[1])
                except AttributeError:
                    player_record = Players(primary_position=position[0], secondary_position=position[1],
                                            **person_dict)
                self.session.add(player_record)
                self.session.commit()
                inserts += 1
                if inserts % 50 == 0:
                    print "{} players inserted".format(inserts)
        print "Player Ingestion complete."
