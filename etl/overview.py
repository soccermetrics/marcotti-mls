import re
import logging
from datetime import date

from models import (Countries, Clubs, DomesticCompetitions, InternationalCompetitions,
                    Persons, Players, NameOrderType, PositionType, ConfederationType)
from etl.base import BaseCSV


logger = logging.getLogger(__name__)


class CountryIngest(BaseCSV):

    def parse_file(self, rows):
        insertion_list = []
        inserts = 0
        logger.info("Ingesting Countries...")
        for keys in rows:
            country_name = "{Name}".format(**keys).strip().decode('utf-8')
            confederation = "{Confederation}".format(**keys).strip()
            if not self.record_exists(Countries, name=country_name):
                country_dict = dict(name=country_name, confederation=ConfederationType.from_string(confederation))
                insertion_list.append(Countries(**country_dict))
                inserted, insertion_list = self.bulk_insert(insertion_list, 50)
                inserts += inserted
        self.session.add_all(insertion_list)
        inserts += len(insertion_list)
        logger.info("Total of {0} Country records inserted and committed to database".format(inserts))
        logger.info("Country Ingestion complete.")


class CompetitionIngest(BaseCSV):

    def parse_file(self, rows):
        inserts = 0
        insertion_list = []
        logger.info("Ingesting Competitions...")
        for keys in rows:
            competition_name = self.column_unicode("Name", **keys)
            level = self.column_int("Level", **keys)
            country_name = self.column_unicode("Country", **keys)
            confederation_name = self.column_unicode("Confederation", **keys)

            if all(var is not None for var in [country_name, confederation_name]):
                logger.error(u"Cannot insert Competition record for {}: "
                             u"Country and Confederation defined".format(competition_name))
                continue
            else:
                comp_record = None
                if country_name is not None:
                    country_id = self.get_id(Countries, name=country_name)
                    comp_dict = dict(name=competition_name, level=level, country_id=country_id)
                    if country_id is None:
                        logger.error(u"Cannot insert Domestic Competition record for {}: "
                                     u"Country {} not found".format(competition_name, country_name))
                        continue
                    elif not self.record_exists(DomesticCompetitions, **comp_dict):
                        comp_record = DomesticCompetitions(**comp_dict)
                elif confederation_name is not None:
                    try:
                        confederation = ConfederationType.from_string(confederation_name)
                    except ValueError:
                        logger.error(u"Cannot insert International Competition record for {}: "
                                     u"Confederation {} not found".format(competition_name, confederation_name))
                        continue
                    comp_dict = dict(name=competition_name, level=level, confederation=confederation)
                    if not self.record_exists(InternationalCompetitions, **comp_dict):
                        comp_record = InternationalCompetitions(**comp_dict)
                else:
                    logger.error(u"Cannot insert Competition record: Neither Country nor Confederation defined")
                    continue
                if comp_record is not None:
                    insertion_list.append(comp_record)
                    logger.debug(u"Adding Competition record: {}".format(comp_dict))
                    inserted, insertion_list = self.bulk_insert(insertion_list, 20)
                    inserts += inserted
        self.session.add_all(insertion_list)
        self.session.commit()
        inserts += len(insertion_list)
        logger.info("Total {} Competition records inserted and committed to database".format(inserts))
        logger.info("Competition Ingestion complete.")


class ClubIngest(BaseCSV):

    def parse_file(self, rows):
        inserts = 0
        insertion_list = []
        logger.info("Ingesting Clubs...")
        for keys in rows:
            club_name = self.column_unicode("Name", **keys)
            club_symbol = self.column("Symbol", **keys)
            country_name = self.column_unicode("Country", **keys)

            if country_name is None:
                logger.error(u"Cannot insert Club record for {}: Country required".format(club_name))
                continue
            else:
                country_id = self.get_id(Countries, name=country_name)
                club_dict = dict(name=club_name, symbol=club_symbol, country_id=country_id)
                if country_id is None:
                    logger.error(u"Cannot insert Club record {}: "
                                 u"Country {} not in database".format(club_dict, country_name))
                elif not self.record_exists(Clubs, **club_dict):
                    insertion_list.append(Clubs(**club_dict))
                    inserted, insertion_list = self.bulk_insert(insertion_list, 50)
                    inserts += inserted
        self.session.add_all(insertion_list)
        self.session.commit()
        inserts += len(insertion_list)
        logger.info("Total {} Club records inserted and committed to database".format(inserts))
        logger.info("Club Ingestion complete.")


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
        date_of_birth = self.column("Birthdate", **keys)
        order = self.column("Name Order", **keys) or "Western"

        name_order = NameOrderType.from_string(order)
        birth_date = date(*tuple(int(x) for x in date_of_birth.split('-'))) if date_of_birth else None

        person_dict = {field: value for (field, value) in zip(
            ['first_name', 'known_first_name', 'middle_name', 'last_name',
             'second_last_name', 'nick_name', 'birth_date', 'order'],
            [first_name, known_first_name, middle_name, last_name,
             second_last_name, nickname, birth_date, name_order]) if value is not None}
        return person_dict


class PlayerIngest(PersonIngest):

    def parse_file(self, rows):
        inserts = 0
        logger.info("Ingesting Players...")
        for keys in rows:
            person_dict = self.get_person_data(**keys)
            position_chars = self.column("Position", **keys)
            country_name = self.column_unicode("Country", **keys)

            country_id = self.get_id(Countries, name=country_name)
            if country_id is None:
                logger.error(u"Cannot insert Player record {}: "
                             u"Country {} not in database".format(person_dict, country_name))
                continue

            position = [PositionType.unknown, None]
            if position_chars is not None:
                position_codes = re.split(r'[;,:\-/]', position_chars)
                for i, code in enumerate(position_codes):
                    position[i] = PositionType.from_string(code)

            person_dict = dict(country_id=country_id, **person_dict)
            if not self.record_exists(Players, **person_dict):
                if not self.record_exists(Persons, **person_dict):
                    player_record = Players(primary_position=position[0], secondary_position=position[1],
                                            **person_dict)
                else:
                    person_id = self.get_id(Persons, **person_dict)
                    player_record = Players(person_id=person_id, primary_position=position[0],
                                            secondary_position=position[1])
                self.session.add(player_record)
                self.session.commit()
                inserts += 1
        logger.info("Total {} Player records inserted and committed to database".format(inserts))
        logger.info("Player Ingestion complete.")
