from typing import Any, Tuple, Optional, Callable

from datajudge import Constraint
from datajudge.db_access import DataReference
import sqlalchemy as sa
from scipy.stats import ks_2samp, kstest


class KolmogorovSmirnov2Sample(Constraint):
    """
    This constraint assures that two given given data references are sampled from the same underlying distribution.
    This is detected by applying the two-sample Kolmogorov-Smirnov test.
    """

    def __init__(self, ref: DataReference, ref2: DataReference, significance_level: float = 0.01):
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

    def get_factual_value(self, engine: sa.engine.Engine) -> Any:
        # get data
        data = self.retrieve(engine, self.ref)
        data2 = self.retrieve(engine, self.ref2)

        # clean data

        # calculate test statistics
        p_value = self.calculate_2sample_ks_test(data, data2)

        return p_value

    def get_target_value(self, engine: sa.engine.Engine) -> Any:
        return self.significance_level

    def compare(
            self, value_factual: Any, value_target: Any
    ) -> Tuple[bool, Optional[str]]:
        # value_factual := calculated p-value from data
        # value_target := required significance level provided by user
        result = value_factual >= value_target
        assertion_text = (
            f"2-Sample Kolmogorov-Smirnov between {self.ref.get_string()} and {self.target_prefix}"
            f"has p-value {value_factual}"
            f"{self.condition_string}"
        )

        return result, assertion_text
