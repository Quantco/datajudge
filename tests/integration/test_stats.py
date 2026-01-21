import pytest

from datajudge._engines import is_bigquery, is_db2
from datajudge.constraints.stats import AndersonDarling2Sample, KolmogorovSmirnov2Sample
from datajudge.data_source import TableDataSource
from datajudge.db_access import (
    DataReference,
    _cross_cdf_selection,
    get_anderson_darling_sums,
    get_row_count,
)


def identity(boolean_value):
    return boolean_value


def negation(boolean_value):
    return not boolean_value


def test_cross_cdf_selection(engine, cross_cdf_table1, cross_cdf_table2):
    # TODO: Fix this
    if is_db2(engine):
        pytest.skip()
    database1, schema1, table1 = cross_cdf_table1
    database2, schema2, table2 = cross_cdf_table2
    tds1 = TableDataSource(database1, table1, schema1)
    tds2 = TableDataSource(database2, table2, schema2)
    ref1 = DataReference(tds1, columns=["col_int"])
    ref2 = DataReference(tds2, columns=["col_int"])
    selection, _, _ = _cross_cdf_selection(engine, ref1, ref2, "cdf", "value")
    with engine.connect() as connection:
        result = connection.execute(selection).fetchall()
    assert result is not None and len(result) > 0
    expected_result = [
        (1, 2 / 4, 0),
        (2, 3 / 4, 0),
        (3, 1, 1 / 5),
        (4, 1, 2 / 5),
        (5, 1, 4 / 5),
        (8, 1, 1),
    ]
    assert sorted(result) == expected_result


@pytest.mark.parametrize(
    "configuration",
    [  # these values were calculated using scipy.stats.ks_2samp on scipy=1.8.1
        ("value_0_1", "value_0_1", 0.0, 1.0),
        ("value_0_1", "value_005_1", 0.0294, 0.00035221594346540835),
        ("value_0_1", "value_02_1", 0.0829, 2.6408848561586672e-30),
        ("value_0_1", "value_1_1", 0.3924, 0.0),
    ],
)
def test_ks_2sample_calculate_statistic(engine, random_normal_table, configuration):
    if is_bigquery(engine) or is_db2(engine):
        pytest.skip("It takes too long to insert the table into BigQuery")

    col_1, col_2, expected_d, expected_p = configuration
    database, schema, table = random_normal_table
    tds = TableDataSource(database, table, schema)
    ref = DataReference(tds, columns=[col_1])
    ref2 = DataReference(tds, columns=[col_2])

    (
        d_statistic,
        p_value,
        n_samples,
        m_samples,
        _,
    ) = KolmogorovSmirnov2Sample._calculate_statistic(engine, ref, ref2)

    assert abs(d_statistic - expected_d) <= 1e-10, (
        f"The test statistic does not match: {expected_d} vs {d_statistic}"
    )

    # 1e-05 should cover common p_values; if scipy is installed, a very accurate p_value is automatically calculated
    assert abs(p_value - expected_p) <= 1e-05, (
        f"The approx. p-value does not match: {expected_p} vs {p_value}"
    )


@pytest.mark.parametrize(
    "configuration",
    [
        (identity, "value_0_1", "value_0_1", 0.05),
        (negation, "value_0_1", "value_005_1", 0.05),
        (negation, "value_0_1", "value_02_1", 0.05),
        (negation, "value_0_1", "value_1_1", 0.05),
    ],
)
def test_ad_2sample_hypothesis_testing(engine, random_normal_table, configuration):
    """Test that the Anderson-Darling 2-sample test correctly rejects null hypothesis for different distributions."""
    if is_bigquery(engine) or is_db2(engine):
        pytest.skip("It takes too long to insert the table into BigQuery/DB2")

    operation, col_1, col_2, significance_level = configuration
    database, schema, table = random_normal_table
    tds = TableDataSource(database, table, schema)
    ref = DataReference(tds, columns=[col_1])
    ref2 = DataReference(tds, columns=[col_2])

    constraint = AndersonDarling2Sample(
        ref, ref2, significance_level=significance_level
    )
    test_result = constraint.test(engine)

    assert operation(test_result.outcome), test_result.failure_message


def test_ad_2sample_similarity_effect_on_statistic(engine, random_normal_table):
    """Test basic a property of the Anderson-Darling test statistic.

    When comparing two similar distributions to two very different distributions,
    the latter should lead to a higher test statistic than the former.
    """
    if is_bigquery(engine) or is_db2(engine):
        pytest.skip("It takes too long to insert the table into BigQuery/DB2")

    database, schema, table = random_normal_table
    tds = TableDataSource(database, table, schema)

    def _statistic(col1, col2):
        ref_small1 = DataReference(tds, columns=["value_0_1"])
        ref_small2 = DataReference(tds, columns=["value_005_1"])

        sample_size1, _ = get_row_count(engine, ref_small1)
        sample_size2, _ = get_row_count(engine, ref_small2)
        sample_size = sample_size1 + sample_size2

        sum1_small, sum2_small, _ = get_anderson_darling_sums(
            engine, ref_small1, ref_small2, sample_size
        )
        return AndersonDarling2Sample.compute_test_statistic(
            sum1_small, sum2_small, sample_size1, sample_size2
        )

    statistic_similar = _statistic("value_0_1", "value_005_1")
    statistic_dissimilar = _statistic("value_0_1", "value_1_1")

    # The statistic for more different distributions should be larger
    assert statistic_dissimilar >= statistic_similar > 0, (
        f"Statistic for large difference ({statistic_dissimilar}) should be "
        f"larger than for small difference ({statistic_similar})"
    )


def test_ad_2sample_critical_value_monotonic_in_sl():
    """Test the critical value approximation function."""
    # Test that critical value increases with decreasing significance level
    cv_025 = AndersonDarling2Sample.approximate_critical_value(100, 100, 0.25)
    cv_01 = AndersonDarling2Sample.approximate_critical_value(100, 100, 0.1)
    cv_005 = AndersonDarling2Sample.approximate_critical_value(100, 100, 0.05)
    cv_001 = AndersonDarling2Sample.approximate_critical_value(100, 100, 0.01)

    assert cv_025 < cv_01 < cv_005 < cv_001, (
        f"Critical values should increase as significance level decreases: "
        f"0.25->{cv_025}, 0.1->{cv_01}, 0.05->{cv_005}, 0.01->{cv_001}"
    )
