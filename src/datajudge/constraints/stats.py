import math
import warnings
from typing import Optional, Tuple

import sqlalchemy as sa

from .. import db_access
from ..db_access import DataReference
from .base import Constraint, TestResult


class KolmogorovSmirnov2Sample(Constraint):
    def __init__(
        self, ref: DataReference, ref2: DataReference, significance_level: float = 0.05
    ):
        self.significance_level = significance_level
        super().__init__(ref, ref2=ref2)

    @staticmethod
    def approximate_p_value(
        d: float, n_samples: int, m_samples: int
    ) -> Optional[float]:
        """
        Calculates the approximate p-value according to
        'A procedure to find exact critical values of Kolmogorov-Smirnov Test', Silvia Fachinetti, 2009

        Note: For environments with `scipy` installed, this method will return a quasi-exact p-value.
        """

        # approximation does not work for small sample sizes
        samples = min(n_samples, m_samples)
        if samples < 35:
            warnings.warn(
                "Approximating the p-value is not accurate enough for sample size < 35"
            )
            return None

        # if scipy is installed, accurately calculate the p_value using the full distribution
        try:
            from scipy.stats.distributions import kstwo

            approx_p = kstwo.sf(
                d, round((n_samples * m_samples) / (n_samples + m_samples))
            )
        except ModuleNotFoundError:
            d_alpha = d * math.sqrt(samples)
            approx_p = 2 * math.exp(-(d_alpha**2))

        # clamp value to [0, 1]
        return 1.0 if approx_p > 1.0 else 0.0 if approx_p < 0.0 else approx_p

    @staticmethod
    def check_acceptance(
        d_statistic: float, n_samples: int, m_samples: int, accepted_level: float
    ) -> bool:
        """
        For a given test statistic, d, and the respective sample sizes `n` and `m`, this function
        checks whether the null hypothesis can be rejected for an accepted significance level.

        For more information, check out the `Wikipedia entry <https://w.wiki/5May>`_.
        """

        def c(alpha: float):
            return math.sqrt(-math.log(alpha / 2.0 + 1e-10) * 0.5)

        return d_statistic <= c(accepted_level) * math.sqrt(
            (n_samples + m_samples) / (n_samples * m_samples)
        )

    @staticmethod
    def calculate_statistic(
        engine,
        ref1: DataReference,
        ref2: DataReference,
    ) -> Tuple[float, Optional[float], int, int]:

        # retrieve test statistic d, as well as sample sizes m and n
        d_statistic = db_access.get_ks_2sample(
            engine,
            ref1,
            ref2,
        )

        n_samples, _ = db_access.get_row_count(engine, ref1)
        m_samples, _ = db_access.get_row_count(engine, ref2)

        # calculate approximate p-value
        p_value = KolmogorovSmirnov2Sample.approximate_p_value(
            d_statistic, n_samples, m_samples
        )

        return d_statistic, p_value, n_samples, m_samples

    def test(self, engine: sa.engine.Engine) -> TestResult:

        # get query selections and column names for target columns

        d_statistic, p_value, n_samples, m_samples = self.calculate_statistic(
            engine,
            self.ref,
            self.ref2,
        )

        # calculate test acceptance
        result = self.check_acceptance(
            d_statistic, n_samples, m_samples, self.significance_level
        )

        assertion_text = (
            f"Null hypothesis (H0) for the 2-sample Kolmogorov-Smirnov test was rejected, i.e., "
            f"the two samples ({self.ref.get_string()} and {self.target_prefix})"
            f" do not originate from the same distribution."
            f"The test results are d={d_statistic}"
        )
        if p_value is not None:
            assertion_text += f"and {p_value=}"

        if not result:
            return TestResult.failure(assertion_text)

        return TestResult.success()
