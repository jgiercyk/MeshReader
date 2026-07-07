"""MQTT root topic classification — country, state, region, type, activity."""
from typing import Dict, Optional

# US census regions
_US_REGIONS: Dict[str, str] = {
    "AL": "South",     "AR": "South",     "DE": "South",     "FL": "South",
    "GA": "South",     "KY": "South",     "LA": "South",     "MD": "South",
    "MS": "South",     "NC": "South",     "OK": "South",     "SC": "South",
    "TN": "South",     "TX": "South",     "VA": "South",     "WV": "South",
    "DC": "South",
    "CT": "Northeast", "MA": "Northeast", "ME": "Northeast", "NH": "Northeast",
    "NJ": "Northeast", "NY": "Northeast", "PA": "Northeast", "RI": "Northeast",
    "VT": "Northeast",
    "IL": "Midwest",   "IN": "Midwest",   "IA": "Midwest",   "KS": "Midwest",
    "MI": "Midwest",   "MN": "Midwest",   "MO": "Midwest",   "NE": "Midwest",
    "ND": "Midwest",   "OH": "Midwest",   "SD": "Midwest",   "WI": "Midwest",
    "AK": "West",      "AZ": "West",      "CA": "West",      "CO": "West",
    "HI": "West",      "ID": "West",      "MT": "West",      "NV": "West",
    "NM": "West",      "OR": "West",      "UT": "West",      "WA": "West",
    "WY": "West",
}

_STATE_NAMES: Dict[str, str] = {
    "AL": "Alabama",        "AK": "Alaska",       "AZ": "Arizona",
    "AR": "Arkansas",       "CA": "California",   "CO": "Colorado",
    "CT": "Connecticut",    "DE": "Delaware",     "FL": "Florida",
    "GA": "Georgia",        "HI": "Hawaii",       "ID": "Idaho",
    "IL": "Illinois",       "IN": "Indiana",      "IA": "Iowa",
    "KS": "Kansas",         "KY": "Kentucky",     "LA": "Louisiana",
    "ME": "Maine",          "MD": "Maryland",     "MA": "Massachusetts",
    "MI": "Michigan",       "MN": "Minnesota",    "MS": "Mississippi",
    "MO": "Missouri",       "MT": "Montana",      "NE": "Nebraska",
    "NV": "Nevada",         "NH": "New Hampshire","NJ": "New Jersey",
    "NM": "New Mexico",     "NY": "New York",     "NC": "North Carolina",
    "ND": "North Dakota",   "OH": "Ohio",         "OK": "Oklahoma",
    "OR": "Oregon",         "PA": "Pennsylvania", "RI": "Rhode Island",
    "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee",
    "TX": "Texas",          "UT": "Utah",         "VT": "Vermont",
    "VA": "Virginia",       "WA": "Washington",   "WV": "West Virginia",
    "WI": "Wisconsin",      "WY": "Wyoming",      "DC": "D.C.",
}


def classify_root(root: str) -> dict:
    """Classify an MQTT root topic string.

    Returns dict with keys: country, state_code, state_name, region, root_type.
    root_type values: national, state, custom, regional, unknown.

    Examples:
      msh/US            → national
      msh/US/SC         → state, SC, South
      msh/US/GA/csramsh → custom, GA, South
      msh/EU            → national (country=EU)
    """
    out: dict = {
        "country":    None,
        "state_code": None,
        "state_name": None,
        "region":     None,
        "root_type":  "unknown",
    }
    parts = root.strip("/").split("/")
    if len(parts) < 2 or parts[0].lower() != "msh":
        return out

    country = parts[1].upper()
    out["country"] = country

    if len(parts) == 2:
        out["root_type"] = "national"
        return out

    seg = parts[2].upper()
    if country == "US" and seg in _US_REGIONS:
        out["state_code"] = seg
        out["state_name"] = _STATE_NAMES.get(seg, seg)
        out["region"]     = _US_REGIONS[seg]
        out["root_type"]  = "state" if len(parts) == 3 else "custom"
    else:
        out["root_type"] = "regional"

    return out


def activity_label(packets_per_minute: float) -> str:
    """Human-readable activity tier from packets/minute rate."""
    if packets_per_minute <= 0:
        return "quiet"
    if packets_per_minute < 0.2:
        return "low"
    if packets_per_minute < 2.0:
        return "medium"
    if packets_per_minute < 20.0:
        return "high"
    return "firehose"
