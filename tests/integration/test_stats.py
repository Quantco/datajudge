import pytest

import datajudge
from datajudge.db_access import (
    DataReference,
    TableDataSource,
    is_bigquery,
    is_db2,
    is_impala,
)


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
    selection, _, _ = datajudge.db_access._cross_cdf_selection(
        engine, ref1, ref2, "cdf", "value"
    )
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
    if is_bigquery(engine) or is_impala(engine) or is_db2(engine):
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
    ) = datajudge.constraints.stats.KolmogorovSmirnov2Sample.calculate_statistic(
        engine, ref, ref2
    )

    assert (
        abs(d_statistic - expected_d) <= 1e-10
    ), f"The test statistic does not match: {expected_d} vs {d_statistic}"

    # 1e-05 should cover common p_values; if scipy is installed, a very accurate p_value is automatically calculated
    assert (
        abs(p_value - expected_p) <= 1e-05
    ), f"The approx. p-value does not match: {expected_p} vs {p_value}"
