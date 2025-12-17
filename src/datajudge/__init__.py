"""datajudge allows to assess  whether data from database complies with reference
information."""

from .constraints.base import Constraint
from .db_access import Condition, DataSource
from .requirements import BetweenRequirement, Requirement, WithinRequirement

__all__ = [
    "BetweenRequirement",
    "Condition",
    "Constraint",
    "DataSource",
    "Requirement",
    "WithinRequirement",
]

__version__ = "1.11.0"
