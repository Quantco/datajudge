# datajudge

[![CI](https://github.com/Quantco/datajudge/actions/workflows/ci.yaml/badge.svg)](https://github.com/Quantco/datajudge/actions/workflows/ci.yaml)
[![Documentation Status](https://readthedocs.org/projects/datajudge/badge/?version=latest)](https://datajudge.readthedocs.io/en/latest/?badge=latest)
[![Conda-forge](https://img.shields.io/conda/vn/conda-forge/datajudge?logoColor=white&logo=conda-forge)](https://anaconda.org/conda-forge/datajudge)
[![PypiVersion](https://img.shields.io/pypi/v/datajudge.svg?logo=pypi&logoColor=white)](https://pypi.org/project/datajudge)


Express and test specifications against data from database.


# Installation instructions

`datajudge` can either be installed via pypi with `pip install datajudge` or via conda-forge with `conda install datajudge -c conda-forge`.

You will likely want to use `datajudge` in conjuction with other packages - in particular `pytest` and database drivers relevant to your database. You might want to install `snowflake-sqlalchemy` when using snowflake, `pyscopg` when using postgres and platform-specific drivers ([Windows](https://docs.microsoft.com/en-us/sql/connect/odbc/windows/microsoft-odbc-driver-for-sql-server-on-windows?view=sql-server-ver15), [Linux](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15), [macOS](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos?view=sql-server-ver15)) when using mssql.

For development _on_ `datajudge` - in contrast to merely using it - you can get started as follows:
```bash
git clone https://github.com/Quantco/datajudge
cd datajudge
mamba env create
conda activate datajudge
pip install --no-build-isolation --disable-pip-version-check -e .
```

# Example


# Usage instructions


## Database management system support

While designed as dbms-agnostically as possible, `datajudge` likely doesn't work in its entirety for every dbms.
We run integration tests against various versions of postgres, mssql and snowflake.


