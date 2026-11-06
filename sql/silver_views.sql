-- One DuckDB view per silver table, reading the Parquet files in place.
-- Run from the project root after `podium-lake transform`:
--   duckdb -init sql/silver_views.sql
-- Facts are partitioned by season: `season` comes from the folder names (season=YYYY).

-- Facts
create or replace view results as
    from read_parquet('data/silver/results/**/*.parquet', hive_partitioning = true);
create or replace view laps as
    from read_parquet('data/silver/laps/**/*.parquet', hive_partitioning = true);
create or replace view pit_stops as
    from read_parquet('data/silver/pit_stops/**/*.parquet', hive_partitioning = true);

-- Dimensions
create or replace view races as from 'data/silver/races/*.parquet';
create or replace view circuits as from 'data/silver/circuits/*.parquet';
create or replace view drivers as from 'data/silver/drivers/*.parquet';
create or replace view constructors as from 'data/silver/constructors/*.parquet';
