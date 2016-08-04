from sqlalchemy.sql import func

from models import *
from base import Analytics


class DraftAnalytics(Analytics):
    """
    Analysis related to MLS player drafts in a specific year.
    """

    DRAFT_TYPES = [AcquisitionType.inaugural_draft, AcquisitionType.college_draft,
                   AcquisitionType.super_draft, AcquisitionType.supplemental_draft]

    def __init__(self, session, yr):
        super(DraftAnalytics, self).__init__(session)
        self.competition_name = u'Major League Soccer'
        self.yr = yr

    @property
    def competition(self):
        return self.session.query(Competitions).filter_by(name=self.competition_name).one()

    @property
    def year(self):
        return self.session.query(Years).filter_by(yr=self.yr).one()

    def mls_acquisition_census(self):
        """
        Return census of MLS acquisition paths of new players.

        :param year: Year object
        :return: list of tuples (acquisition path, number of players)
        """
        return self.session.query(AcquisitionPaths.path, func.count(AcquisitionPaths.player_id)).filter(
            AcquisitionPaths.year == self.year).group_by(AcquisitionPaths.path)

    def mls_drafts_available(self):
        """
        Return the MLS drafts available for new players.

        :return:
        """
        paths_year = zip(*self.session.query(AcquisitionPaths.path.distinct()).filter(
            AcquisitionPaths.year == self.year).all())[0]
        return set(DraftAnalytics.DRAFT_TYPES) & set(paths_year)

    def mls_draft_format(self, dtype):
        """
        Return number of rounds in draft, given draft type.

        :param dtype: Draft type (AcquisitionType)
        :return:
        """
        return self.session.query(func.max(PlayerDrafts.round), func.max(PlayerDrafts.selection)).filter(
            PlayerDrafts.path == dtype, PlayerDrafts.year == self.year).one()

    def draft_class(self, rnd, dtype=None):
        """
        Return draft class records given draft round and draft type (optional).

        If draft type is None, then all records for the draft round of all drafts in the year are retrieved.

        :param rnd: Draft round
        :param dtype: Draft type (AcquisitionType) or None.
        :return:
        """
        params = dict(year=self.year, round=rnd)
        if dtype is not None:
            params.update(path=dtype)
        return self.session.query(PlayerDrafts).filter_by(**params)

    def draft_class_by_position(self, rnd, dtype=None, **kwargs):
        """
        Return draft class records given draft round, draft type (optional), and primary and/or secondary positions.

        If draft type is None, then all records for the draft round of all drafts from that year are retrieved.

        :param rnd: Draft round
        :param dtype: Draft type (AcquisitionType) or None.
        :param kwargs: Primary/secondary positions (PositionType).
        :return:
        """
        return self.draft_class(rnd, dtype).join(Players).filter_by(**kwargs)

    def history(self, player_id):
        """
        Return all draft records of player, sorted by year.

        :param player_id: Unique player ID
        :return:
        """
        return self.session.query(PlayerDrafts).filter_by(player_id=player_id).order_by(PlayerDrafts.year_id)

    def is_initial_draft(self, player_id, current_year):
        """
        Return boolean that indicates whether a player was drafted for the first time in a given year.

        :param player_id: Unique player ID
        :param current_year: Draft year (int)
        :return:
        """
        draft_history = self.history(player_id)
        if draft_history.count() > 1:
            initial_draft_year = self.session.query(Years).get(draft_history.first().year_id).yr
            return True if initial_draft_year == current_year else False
        else:
            return True

    def draft_mortality(self, rnd, dtype=None, seasons=None):
        """
        Calculate number of drafted players who do not appear in a MLS match.

        If dtype is None, consider all drafts that occur in the year.

        If seasons is None, consider all seasons of competition.

        :param rnd: Draft rounds
        :param dtype: Draft type (AcquisitionType) or None
        :param seasons: Seasons after draft year or None
        :return:
        """
        draft_class = [rec.player_id for rec in self.draft_class(rnd, dtype)]
        stat_players = self.session.query(CommonStats.player_id)
        if seasons is None:
            stat_players = stat_players.filter(CommonStats.competition_id == self.competition.id).all()
        else:
            stat_players = stat_players.filter(CommonStats.season_id.in_(seasons)).all()
        stat_players = list(sum(stat_players, ()))
        total_drafted = len(set(draft_class))
        not_appearing = len(set(draft_class) - set(stat_players))
        appearing = total_drafted - not_appearing
        return total_drafted, not_appearing, appearing
