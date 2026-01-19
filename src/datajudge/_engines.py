import sqlalchemy as sa


def is_mssql(engine: sa.engine.Engine) -> bool:
    return engine.name == "mssql"


def is_postgresql(engine: sa.engine.Engine) -> bool:
    return engine.name == "postgresql"


def is_snowflake(engine: sa.engine.Engine) -> bool:
    return engine.name == "snowflake"


def is_bigquery(engine: sa.engine.Engine) -> bool:
    return engine.name == "bigquery"


def is_db2(engine: sa.engine.Engine) -> bool:
    return engine.name == "ibm_db_sa"


def is_duckdb(engine: sa.engine.Engine) -> bool:
    return engine.name == "duckdb"
