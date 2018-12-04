from marcottimls.models.common import DeclEnum


class ConfederationType(DeclEnum):
    """
    Enumerated names of the international football confederations.
    """
    africa = "CAF", "Confederation of African Football"
    asia = "AFC", "Asian Football Confederation"
    europe = "UEFA", "Union of European Football Associations"
    north_america = "CONCACAF", "Confederation of North, Central American, and Caribbean Association Football"
    oceania = "OFC", "Oceania Football Confederation"
    south_america = "CONMEBOL", "South American Football Confederation"
    fifa = "FIFA", "International Federation of Association Football"


class PositionType(DeclEnum):
    """
    Enumerated categories of football player positions.
    """
    goalkeeper = "GK", "Goalkeepers"
    defender = "D", "Defending positions"
    midfielder = "M", "Midfield positions"
    forward = "F", "Forward positions"
    unknown = "U", "Unknown player position"


class NameOrderType(DeclEnum):
    """
    Enumerated types of naming order conventions.
    """
    western = "Western", "Western"
    middle = "Middle", "Middle"
    eastern = "Eastern", "Eastern"


class AcquisitionType(DeclEnum):
    """
    Enumerated types of MLS acquisition paths.
    """
    inaugural_draft = "Inaugural Draft", "Inaugural player draft"
    college_draft = "College Draft", "College player draft"
    supplemental_draft = "Supplemental Draft", "Supplemental player draft"
    super_draft = "SuperDraft", "SuperDraft"
    waiver_draft = "Waiver Draft", "Waiver draft"
    inaugural_allocation = "Inaugural Allocation", "Inaugural allocation"
    allocation = "Allocation Signing", "Allocation signing"
    designated = "Designated Player", "Designated Player Rule"
    developmental = "Developmental Contract", "Developmental player contract"
    discovery = "Discovery Player", "Discovery player contract"
    domestic = "Domestic Signing", "Domestic player contract"
    emergency = "Emergency Loan", "Emergency Loan"
    foreign = "Foreign Signing", "Foreign player contract"
    homegrown = "Homegrown Player", "Homegrown Player Rule"
    loan = "Loan Signing", "Loan signing"
    project40 = "Project-40 Allocation", "Project-40 allocation"
    weighted_lottery = "Weighted Lottery", "Weighted Lottery"
    tam_loan = "TAM Loan Signing", "Loan signing - Targeted Allocation Money"
    tam_domestic = "TAM Domestic Signing", "Domestic signing - Targeted Allocation Money"
    tam_foreign = "TAM Foreign Signing", "Foreign signing - Targeted Allocation Money"
    tam_discovery = "TAM Discovery Signing", "Discovery signing - Targeted Allocation Money"
