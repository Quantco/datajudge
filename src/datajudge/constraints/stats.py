import math
import warnings
from typing import List, Optional, Tuple

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

        threshold = c(accepted_level) * math.sqrt(
            (n_samples + m_samples) / (n_samples * m_samples)
        )
        return d_statistic <= threshold

    @staticmethod
    def calculate_statistic(
        engine,
        ref1: DataReference,
        ref2: DataReference,
    ) -> Tuple[float, Optional[float], int, int, List]:

        # retrieve test statistic d, as well as sample sizes m and n
        d_statistic, ks_selections = db_access.get_ks_2sample(
            engine,
            ref1,
            ref2,
        )

        n_samples, n_selections = db_access.get_row_count(engine, ref1)
        m_samples, m_selections = db_access.get_row_count(engine, ref2)

        # calculate approximate p-value
        p_value = KolmogorovSmirnov2Sample.approximate_p_value(
            d_statistic, n_samples, m_samples
        )

        selections = n_selections + m_selections + ks_selections
        return d_statistic, p_value, n_samples, m_samples, selections

    def test(self, engine: sa.engine.Engine) -> TestResult:
        (
            d_statistic,
            p_value,
            n_samples,
            m_samples,
            selections,
        ) = self.calculate_statistic(
            engine,
            self.ref,
            self.ref2,
        )
        result = self.check_acceptance(
            d_statistic, n_samples, m_samples, self.significance_level
        )

        assertion_text = (
            f"Null hypothesis (H0) for the 2-sample Kolmogorov-Smirnov test was rejected, i.e., "
            f"the two samples ({self.ref.get_string()} and {self.target_prefix}) "
            f"do not originate from the same distribution. "
            f"The test results are d={d_statistic}"
        )
        if p_value is not None:
            assertion_text += f" and {p_value=}"
        assertion_text += "."

        if selections:
            queries = [
                str(selection.compile(engine, compile_kwargs={"literal_binds": True}))
                for selection in selections
            ]

        if not result:
            return TestResult.failure(
                assertion_text,
                self.get_description(),
                queries,
            )

        return TestResult.success()


class AndersonDarling2Sample(Constraint):
    def __init__(
        self, ref: DataReference, ref2: DataReference, significance_level: float = 0.05
    ):
        self.significance_level = significance_level
        super().__init__(ref, ref2=ref)

    @staticmethod
    def approximate_critical_value(
        size_sample1: int, size_sample2: int, significance_level: float
    ) -> float:
        """Approximate critical value for specific significance_level given sample sizes."""
        coefficient_map = {
            0.25: {"b0": 0.675, "b1": -0.245, "b2": -0.105},
            0.1: {"b0": 1.281, "b1": 0.25, "b2": -0.305},
            0.05: {"b0": 1.645, "b1": 0.678, "b2": -0.362},
            0.025: {"b0": 1.96, "b1": 1.149, "b2": -0.391},
            0.01: {"b0": 2.326, "b1": 1.822, "b2": -0.396},
            0.005: {"b0": 2.573, "b1": 2.364, "b2": -0.345},
            0.001: {"b0": 3.085, "b1": 3.615, "b2": -0.154},
        }

        if significance_level not in coefficient_map.keys():
            raise KeyError(
                f"Significance-level {significance_level} not supported."
                f" Please choose one of {coefficient_map.keys()}."
            )

        coefficients = coefficient_map[significance_level]
        b0 = coefficients["b0"]
        b1 = coefficients["b1"]
        b2 = coefficients["b2"]
        critical_value = b0 + b1 / math.sqrt(size_sample1) + b2 / size_sample1
        return critical_value

    @staticmethod
    def compute_test_statistic(sum1, sum2, sample_size1, sample_size2):
        sample_size = sample_size1 + sample_size2
        return ((sum1 * sample_size1) + (sum2 * sample_size2)) / sample_size

    def test(self, engine: sa.engine.Engine) -> TestResult:
        sample_size1, sample_size1_selections = db_access.get_row_count(
            engine, self.ref
        )
        sample_size2, sample_size2_selections = db_access.get_row_count(
            engine, self.ref2
        )
        sample_size = sample_size1 + sample_size2
        sum1, sum2, sum_selections = db_access.get_anderson_darling_sums(
            engine, self.ref, self.ref2, sample_size
        )
        test_statistic = self.compute_test_statistic(
            sum1, sum2, sample_size1, sample_size2
        )
        critical_value = self.approximate_critical_value(
            sample_size1, sample_size2, self.significance_level
        )
        result = test_statistic <= critical_value
        assertion_text = ""
        if not result:
            return TestResult.failure(
                assertion_text,
                self.get_description(),
                sum_selections + sample_size1_selections + sample_size2_selections,
            )
        return TestResult.success()
