"""datajudge allows to assess  whether data from database complies with reference
information."""

import importlib.metadata

from .constraints.base import Constraint
from .db_access import Condition
from .requirements import BetweenRequirement, Requirement, WithinRequirement

__all__ = [
    "BetweenRequirement",
    "Condition",
    "Constraint",
    "Requirement",
    "WithinRequirement",
]

try:
    __version__ = importlib.metadata.distribution(__name__).version
except Exception:
    __version__ = "1.9.0"
