from typing import Any, Collection, Optional, Tuple

import sqlalchemy as sa

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
    def calculate_2sample_ks_test(data: Collection, data2: Collection) -> float:
        """
        For two given lists of values calculates the Kolmogorov-Smirnov test.
        Read more here: https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.kstest.html
        """
        try:
            from scipy.stats import ks_2samp
        except ModuleNotFoundError:
            raise ModuleNotFoundError(
                "Calculating the Kolmogorov-Smirnov test relies on scipy."
                "Therefore, please install scipy before using this test."
            )

        # Currently, the calculation will be performed locally through scipy
        # In future versions, an implementation where either the database engine
        # (1) calculates the CDF
        # or even (2) calculates the KS test
        # can be expected
        statistic, p_value = ks_2samp(data, data2)

        return p_value

    def retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> Tuple[Any, OptionalSelections]:
        return db_access.get_column(engine, ref)

    def compare(
        self, value_factual: Any, value_target: Any
    ) -> Tuple[bool, Optional[str]]:

        p_value = self.calculate_2sample_ks_test(value_factual, value_target)
        result = p_value >= self.significance_level
        assertion_text = (
            f"2-Sample Kolmogorov-Smirnov between {self.ref.get_string()} and {self.target_prefix}"
            f"has p-value {p_value}  < {self.significance_level}"
            f"{self.condition_string}"
        )

        return result, assertion_text
