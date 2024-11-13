import duckdb

conn = duckdb.connect()

conn.sql("""
CREATE TABLE IF NOT EXISTS units (
         name VARCHAR PRIMARY KEY,
         long_name VARCHAR,
         -- symbol VARCHAR,
         aliases VARCHAR[],
);
INSERT INTO units (name, long_name, aliases) VALUES ('cms', 'Cubic Meters Per Second', ['m^3/s', 'm3/s', 'm3 s-1']);
""")

conn.sql("""
    COPY (SELECT * FROM units) TO 'units.csv' WITH (HEADER, DELIMITER '|');
""")

# This works
res = conn.sql("""
    SELECT * FROM read_csv('units.csv', delim = '|', header = true, columns = {'name': 'VARCHAR','long_name': 'VARCHAR','aliases': 'VARCHAR[]'});
""")
print(res)

conn.sql("""TRUNCATE units;""")

conn.sql("""
INSERT INTO units SELECT * FROM read_csv('units.csv', delim = '|',header = true, columns = {'name': 'VARCHAR','long_name': 'VARCHAR','aliases': 'VARCHAR[]'});
""")

units = conn.sql("SELECT * FROM units;")
print(units)

conn.close()