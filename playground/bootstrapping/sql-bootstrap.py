import duckdb
conn = duckdb.connect()

res = conn.sql("""
    CREATE SEQUENCE seq_personid START 1;
    CREATE TABLE cats (
        id INTEGER PRIMARY KEY,
        name VARCHAR,
        mass FLOAT
    );
    INSERT INTO cats (id, name, mass) VALUES
        (nextval('seq_personid'), 'Apollo', 3.2),
        (nextval('seq_personid'), 'Bean', 2.4),
        (nextval('seq_personid'), 'Casper', 6.9),
        (nextval('seq_personid'), 'Daisy', 3.2),
        (nextval('seq_personid'), 'Ella', 5.1),
        (nextval('seq_personid'), 'Finn', 3.5),
        (nextval('seq_personid'), 'Ginger', 5.9),
        (nextval('seq_personid'), 'Harley', 3.3),
        (nextval('seq_personid'), 'Iago', 5.5),
        (nextval('seq_personid'), 'Jasper', 5.4)
;""")
# print(res)

res = conn.sql("""
WITH bootstrap_indexes AS (
  SELECT unnest(generate_series(1, 5)) AS bootstrap_index
),
bootstrap_data AS (
  SELECT mass, ROW_NUMBER() OVER (ORDER BY id) - 1 AS data_index
  FROM cats
),
bootstrap_map AS (
  SELECT floor(
    random() * (SELECT count(data_index) FROM bootstrap_data)
  ) AS data_index,
    bootstrap_index
  FROM bootstrap_data
  JOIN bootstrap_indexes ON TRUE
),
bootstrap AS (
  SELECT bootstrap_index, avg(mass) AS mass_avg
  FROM bootstrap_map
  JOIN bootstrap_data USING (data_index)
  GROUP BY bootstrap_index
),
bootstrap_ci AS (
  SELECT
    percentile_cont(0.05) WITHIN GROUP (ORDER BY mass_avg) AS mass_05pct,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY mass_avg) AS mass_95pct
  FROM bootstrap
),
sample AS (
  SELECT avg(mass) AS mass_avg
  FROM cats
)
SELECT * FROM sample JOIN bootstrap_ci ON TRUE
;""")
print(res)

# res = conn.sql("""
# WITH bootstrap_indexes AS (
#   SELECT generate_series(1, 5) AS bootstrap_index
# ),
# bootstrap_data AS (
#   SELECT mass, ROW_NUMBER() OVER (ORDER BY id) - 1 AS data_index
#   FROM cats
# ),
# bootstrap_map AS (
#   SELECT floor(
#     random() * (SELECT count(data_index) FROM bootstrap_data)
#   ) AS data_index,
#     bootstrap_index
#   FROM bootstrap_data
#   JOIN bootstrap_indexes ON TRUE
# ),
# bootstrap AS (
#   SELECT bootstrap_index, avg(mass) AS mass_avg
#   FROM bootstrap_map
#   JOIN bootstrap_data USING (data_index)
#   GROUP BY bootstrap_index
# ),
# bootstrap_ci AS (
#   SELECT
#     percentile_cont(0.025) WITHIN GROUP (ORDER BY mass_avg) AS mass_lo,
#     percentile_cont(0.975) WITHIN GROUP (ORDER BY mass_avg) AS mass_hi
#   FROM bootstrap
# ),
# sample AS (
#   SELECT avg(mass) AS mass_avg
#   FROM cats
# )
# --SELECT * FROM sample JOIN bootstrap_ci ON TRUE
# SELECT * FROM bootstrap_map
# ;""")
# print(res)

# res = conn.sql("""
# CREATE SEQUENCE serial START WITH 1 MAXVALUE 10;
# WITH bootstrap_indexes AS (
#     SELECT unnest(generate_series(1, 5)) AS bootstrap_index
# ),
# bootstrap_data AS (
#     SELECT * FROM serial USING SAMPLE 5
#     --JOIN bootstrap_indexes ON TRUE
# )
# SELECT * FROM bootstrap_data
# ;""")
# print(res)
