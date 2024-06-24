Development
===========

``datajudge`` development relies on [pixi](https://pixi.sh/latest/).
In order to work on ``datajudge``, you can create a development environment as follows:

::

    git clone https://github.com/Quantco/datajudge
    cd datajudge
    pixi run postinstall

Unit tests can be run by executing

::

   pixi run test

Integration tests are run against a specific backend at a time. As of now, we provide helper
scripts to spin up either a Postgres or MSSQL backend.

To run integration tests against Postgres, first start a docker container with a Postgres database:

::

   ./start_postgres.sh

In your current environment, install the ``psycopg2`` package.
After this, you may execute integration tests as follows:

::

   pixi run -e postgres-py38 test

Analogously, for MSSQL, run

::

   ./start_mssql.sh

and

::

   pixi run -e mssql-py310 test

or


::

   pixi run -e mssql-py310 test_freetds


depending on the driver you'd like to use.
