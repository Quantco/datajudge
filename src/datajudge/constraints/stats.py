from typing import Any, Optional, Tuple

import sqlalchemy as sa
from scipy.stats import ks_2samp

from .. import db_access
from ..db_access import DataReference
from .base import Constraint, OptionalSelections


class KolmogorovSmirnov2Sample(Constraint):
    def __init__(
        self, ref: DataReference, ref2: DataReference, significance_level: float = 0.05
    ):
        self.significance_level = significance_level
        super().__init__(ref, ref2=ref2)

    @staticmethod
    def calculate_2sample_ks_test(data, data2):
        """
        For two given lists of values calculates the Kolmogorov-Smirnov test.
        Read more here: https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.kstest.html
        """
        # data validation:
        # TODO: perform checks that are needed for scipy's test function

        # calculate statistic
        statistic, p_value = ks_2samp(data, data2)

        return p_value

    def retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> Tuple[Any, OptionalSelections]:
        return db_access.get_column(engine, ref)

    def compare(
        self, value_factual: Any, value_target: Any
    ) -> Tuple[bool, Optional[str]]:
        # factual and target values are the corresponding columns from our data source
        p_value = self.calculate_2sample_ks_test(value_factual, value_target)
        result = p_value >= self.significance_level
        assertion_text = (
            f"2-Sample Kolmogorov-Smirnov between {self.ref.get_string()} and {self.target_prefix}"
            f"has p-value {p_value}  < {self.significance_level}"
            f"{self.condition_string}"
        )

        return result, assertion_text
