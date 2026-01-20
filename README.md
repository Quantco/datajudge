# datajudge

[![CI](https://github.com/Quantco/datajudge/actions/workflows/ci.yaml/badge.svg)](https://github.com/Quantco/datajudge/actions/workflows/ci.yaml)
[![Documentation Status](https://readthedocs.org/projects/datajudge/badge/?version=latest)](https://datajudge.readthedocs.io/en/latest/?badge=latest)
[![Conda-forge](https://img.shields.io/conda/vn/conda-forge/datajudge?logoColor=white&logo=conda-forge)](https://anaconda.org/conda-forge/datajudge)
[![PypiVersion](https://img.shields.io/pypi/v/datajudge.svg?logo=pypi&logoColor=white)](https://pypi.org/project/datajudge)
[![codecov.io](https://codecov.io/github/QuantCo/datajudge/coverage.svg?branch=main)](https://codecov.io/github/QuantCo/datajudge?branch=main)

Express and test specifications against data from database.

[Documentation](https://datajudge.readthedocs.io/en/latest/index.html)

# Supported backends

![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?logo=postgresql&logoColor=white)
![Microsoft SQL Server](https://img.shields.io/badge/SQL_Server-CC2927?logo=microsoftsqlserver&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)
![Google BigQuery](https://img.shields.io/badge/BigQuery-669DF6?logo=googlebigquery&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?logo=duckdb&logoColor=black)

# Usage

`datajudge` can either be installed via pypi with `pip install datajudge` or via conda-forge with `conda install datajudge -c conda-forge`.

Please refer to the [Getting Started](https://datajudge.readthedocs.io/en/latest/getting-started/) section of our documentation for details.

Expressing an expectations between different tables from a database may look as such:

```python
from datajudge import BetweenRequirement

companies_between_req = BetweenRequirement.from_tables(
    db_name1="example",
    table_name1="companies",
    db_name2="example",
    table_name2="companies_archive",
)

companies_between_req.add_row_superset_constraint(
    columns1=["name"], columns2=["name"], constant_max_missing_fraction=0
)
```
