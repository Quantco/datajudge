# Before executing lines of this script at a time, run `/start_postrgres.sh`.

import os

import sqlalchemy as sa

# We will first connect to database with the default user.
# Once connected, we will create two tables and a user 'jim'.
# jim should have all rights for table 'companies' but no
# rights for table 'companies_secret'.

# Step 1: Set up the database.

address = os.environ.get("DB_ADDR", "localhost")
connection_string = f"postgresql://datajudge:datajudge@{address}:5432/datajudge"
engine = sa.create_engine(connection_string)

with engine.connect() as con:
    con.execute(
        "CREATE TABLE companies (id INTEGER PRIMARY KEY, name TEXT, num_employees INTEGER)"
    )
    con.execute(
        "INSERT INTO companies (id, name, num_employees) VALUES (1, 'QuantCo', 100), (2, 'Google', 150000), (3, 'BMW', 120000), (4, 'Apple', 145000)"
    )

    con.execute(
        "CREATE TABLE companies_secret (id INTEGER PRIMARY KEY, name TEXT, num_employees INTEGER)"
    )
    con.execute(
        "INSERT INTO companies_secret (id, name, num_employees) VALUES (1, 'QuantCo', 100), (2, 'Google', 150000), (3, 'BMW', 120000), (4, 'Apple', 145000)"
    )

    con.execute("CREATE ROLE jim  LOGIN PASSWORD 'jim'")
    con.execute("GRANT ALL on companies TO jim")
    con.execute("REVOKE ALL ON companies_secret FROM jim")


# Step 2: Query the database.

connection_string_jim = f"postgresql://jim:jim@{address}:5432/datajudge"
engine_jim = sa.create_engine(connection_string_jim)

with engine_jim.connect() as con:
    result1 = con.execute("SELECT * FROM companies").fetchall()

print(result1)


# This fails.
with engine_jim.connect() as con:
    result2 = con.execute("SELECT * FROM companies_secret").fetchall()

print(result2)

metadata = sa.MetaData(engine_jim)

# Both of these succeed. Even though jim doesn't have rights to read from
# 'companies_secret', all of its metadata around columns, constraints etc.
# can be investigaed.
table = sa.Table("companies", metadata, autoload_with=engine_jim)
table_secret = sa.Table("companies_secret", metadata, autoload_with=engine_jim)

with engine_jim.connect() as con:
    try:
        result2 = con.execute("SELECT * FROM companies").first()
    except sa.exc.ProgrammingError as error:
        if "psycopg2.errors.InsufficientPrivilege" in str(error):
            print("missing privilege")
        else:
            raise error


with engine.connect() as con:
    result = con.execute(
        "SELECT table_name, grantee, privilege_type FROM information_schema.table_privileges WHERE grantee = 'jim'"
    ).fetchall()
print(result)

# This outputs:
# [('companies', 'jim', 'INSERT'),
#  ('companies', 'jim', 'SELECT'),
#  ('companies', 'jim', 'UPDATE'),
#  ('companies', 'jim', 'DELETE'),
#  ('companies', 'jim', 'TRUNCATE'),
#  ('companies', 'jim', 'REFERENCES'),
#  ('companies', 'jim', 'TRIGGER')]
