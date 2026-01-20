"""datajudge allows to assess  whether data from database complies with referenceinformation."""

import warnings

import sqlalchemy as sa

if sa.__version__.startswith("1."):
    warnings.warn(
        "SQLAlchemy 1.x is deprecated and will no longer be supported in future "
        "versions of datajudge. Please upgrade to SQLAlchemy 2.x.",
        FutureWarning,
        stacklevel=2,
    )

from .condition import Condition
from .constraints.base import Constraint
from .data_source import DataSource
from .requirements import BetweenRequirement, Requirement, WithinRequirement

__all__ = [
    "BetweenRequirement",
    "Condition",
    "Constraint",
    "DataSource",
    "Requirement",
    "WithinRequirement",
]

__version__ = "1.13.0"
