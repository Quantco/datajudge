import math
import warnings
from typing import Any, Tuple

import sqlalchemy as sa

from .. import db_access
from ..db_access import DataReference
from .base import Constraint, OptionalSelections, TestResult


class KolmogorovSmirnov2Sample(Constraint):
    def __init__(
        self, ref: DataReference, ref2: DataReference, significance_level: float = 0.05
    ):
        self.significance_level = significance_level
        super().__init__(ref, ref2=ref2)

    def retrieve(
        self, engine: sa.engine.Engine, ref: DataReference
    ) -> Tuple[Any, OptionalSelections]:
        sel = ref.get_selection(engine)  # table selection incl. WHERE condition
        col = ref.get_column(engine)  # column name
        return sel, col

    @staticmethod
    def approximate_p_value(d: float, m: int, n: int) -> float | None:
        """
        Calculates the approximate p-value according to
        'A procedure to find exact critical values of Kolmogorov-Smirnov Test', Silvia Fachinetti, 2009
        """

        n = max(m, n) // min(m, n)
        if n < 35:
            warnings.warn(
                "Approximating the p-value is not accurate enough for sample size < 35"
            )
            return None

        d_alpha = d * math.sqrt(n)
        return -2 * math.exp(-(d_alpha**2))

    @staticmethod
    def check_acceptance(d: float, n: int, m: int, accepted_level):
        def c(alpha: float):
            return math.sqrt(-math.log(alpha / 2.0) * 0.5)

        # source: https://en.wikipedia.org/wiki/Kolmogorov%E2%80%93Smirnov_test
        return d <= c(accepted_level) * math.sqrt((n + m) / (n * m))

    def test(self, engine: sa.engine.Engine) -> TestResult:

        # get query selections and column names for target columns
        selection1 = self.ref.get_selection(engine)
        column1 = self.ref.get_column(engine)
        selection2 = self.ref2.get_selection(engine)
        column2 = self.ref2.get_column(engine)

        # retrieve test statistic d, as well as sample sizes m and n
        d_statistic, m, n = db_access.get_ks_2sample(
            engine, table1=(selection1, column1), table2=(selection2, column2)
        )

        # calculate approximate p-value
        p_value = self.approximate_p_value(d_statistic, m, n)

        # calculate test acceptance
        result = self.check_acceptance(d_statistic, n, m, self.significance_level)

        assertion_text = (
            f"Null hypothesis (H0) for the 2-sample Kolmogorov-Smirnov test was rejected, i.e., "
            f"the two samples ({self.ref.get_string()} and {self.target_prefix})"
            f" do not originate from the same distribution."
        )
        if p_value:
            assertion_text += f"\n p-value: {p_value}"

        if not result:
            return TestResult.failure(assertion_text)

        return TestResult.success()
