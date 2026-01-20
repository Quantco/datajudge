# Development

`datajudge` development relies on [pixi](https://pixi.sh/latest/).
In order to work on `datajudge`, you can create a development environment as follows:

```bash
git clone https://github.com/Quantco/datajudge
cd datajudge
pixi run postinstall
```

Unit tests can be run by executing

```bash
pixi run test
```

Integration tests are run against a specific backend at a time. As of now, we provide helper
scripts to spin up either a Postgres or MSSQL backend.

To run integration tests against Postgres, first start a docker container with a Postgres database:

```bash
./start_postgres.sh
```

Then, you can run tests against the database you just started with one of the Postgres-specific
pixi environments, e.g.:

```bash
pixi run -e postgres-py312 test
```

Analogously, for MSSQL, run

```bash
./start_mssql.sh
```

and

```bash
pixi run -e mssql-py312 test
```

or

```bash
pixi run -e mssql-py312 test_freetds
```

depending on the driver you'd like to use.

Please note that running tests against Snowflake and BigQuery requires authentication to be
set up properly.
