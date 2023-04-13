Development
===========

In order to work on ``datajudge``, you can create a development conda environment as follows:

::

    git clone https://github.com/Quantco/datajudge
    cd datajudge
    mamba env create
    conda activate datajudge
    pip install --no-build-isolation --disable-pip-version-check -e .

Unit tests can be run by executing

::

   pytest tests/unit

Integration tests are run against a specific backend at a time. As of now, we provide helper
scripts to spin up either a Postgres or MSSQL backend.
Our ``environment.yml`` does not include the necessary dependencies for these backends, so you will need to install them manually.


To run integration tests against Postgres, first start a docker container with a Postgres database:

::

   ./start_postgres.sh

In your current environment, install the ``psycopg2`` package.
After this, you may execute integration tests as follows:

::

   pytest tests/integration --backend=postgres

Analogously, for MSSQL, run

::

   ./start_mssql.sh

and

::

   pytest tests/integration --backend=mssql-freetds

or

::

   pytest tests/integration --backend=mssql

depending on the driver you are using.

