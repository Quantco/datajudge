"""`Datajudge` allows to assess  whether data from database complies with reference
information."""


import pkg_resources

from .constraints.base import Constraint
from .db_access import Condition
from .requirements import (
    BetweenRequirement,
    BetweenTableRequirement,
    Requirement,
    WithinRequirement,
    WithinTableRequirement,
)

__all__ = [
    "BetweenRequirement",
    "BetweenTableRequirement",
    "Condition",
    "Constraint",
    "Requirement",
    "WithinRequirement",
    "WithinTableRequirement",
]

try:
    __version__ = pkg_resources.get_distribution(__name__).version
except Exception:
    __version__ = "1.0.0"
