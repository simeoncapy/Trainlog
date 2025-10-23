BEGIN;

CREATE EXTENSION IF NOT EXISTS postgis;

-- Core tables.

CREATE TABLE IF NOT EXISTS admin_area (
  admin_area_id         BIGSERIAL PRIMARY KEY,
  iso_code              TEXT UNIQUE,
  level                 SMALLINT NOT NULL CHECK (level IN (1,2)),
  parent_admin_area_id  BIGINT REFERENCES admin_area(admin_area_id) ON DELETE CASCADE,
  name                  TEXT NOT NULL,
  geom                  geometry(MULTIPOLYGON, 4326)
);

CREATE INDEX IF NOT EXISTS admin_area_level_idx  ON admin_area(level);
CREATE INDEX IF NOT EXISTS admin_area_parent_idx ON admin_area(parent_admin_area_id);
CREATE INDEX IF NOT EXISTS admin_area_geom_gix   ON admin_area USING GIST (geom);

CREATE TABLE IF NOT EXISTS coverage_unit (
  unit_id               BIGSERIAL PRIMARY KEY,
  admin_area_id         BIGINT NOT NULL REFERENCES admin_area(admin_area_id) ON DELETE CASCADE,
  source_feature_id     TEXT,
  geom                  geometry(MULTIPOLYGON, 4326) NOT NULL,
  area_m2               DOUBLE PRECISION GENERATED ALWAYS AS (ST_Area(geom::geography)) STORED,
  properties_json       JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS coverage_unit_area_src_uidx
  ON coverage_unit (admin_area_id, source_feature_id);

CREATE INDEX IF NOT EXISTS coverage_unit_geom_gix ON coverage_unit USING GIST (geom);
CREATE INDEX IF NOT EXISTS coverage_unit_area_idx ON coverage_unit(admin_area_id);

CREATE TABLE IF NOT EXISTS user_traveled_unit (
  username              TEXT NOT NULL,
  unit_id               BIGINT NOT NULL REFERENCES coverage_unit(unit_id) ON DELETE CASCADE,
  first_seen_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (username, unit_id)
);

CREATE INDEX IF NOT EXISTS user_traveled_unit_unit_idx ON user_traveled_unit(unit_id);
CREATE INDEX IF NOT EXISTS user_traveled_unit_seen_idx ON user_traveled_unit(first_seen_at);

-- Cache table for region-to-country-unit intersection areas.
CREATE TABLE IF NOT EXISTS coverage_unit_area_by_area (
  admin_area_id         BIGINT NOT NULL REFERENCES admin_area(admin_area_id) ON DELETE CASCADE,
  unit_id               BIGINT NOT NULL REFERENCES coverage_unit(unit_id) ON DELETE CASCADE,
  area_in_area_m2       DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (admin_area_id, unit_id)
);

CREATE INDEX IF NOT EXISTS cuaba_area_idx ON coverage_unit_area_by_area(admin_area_id);
CREATE INDEX IF NOT EXISTS cuaba_unit_idx ON coverage_unit_area_by_area(unit_id);

-- Function to populate cache for a single region (level=2).
CREATE OR REPLACE FUNCTION populate_region_unit_areas(p_region_id BIGINT)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_level SMALLINT;
BEGIN
  SELECT level INTO v_level FROM admin_area WHERE admin_area_id = p_region_id;
  IF v_level IS NULL THEN
    RAISE EXCEPTION 'admin_area_id % not found', p_region_id;
  END IF;
  IF v_level <> 2 THEN
    RAISE EXCEPTION 'Expected level=2 region, got level=%', v_level;
  END IF;

  DELETE FROM coverage_unit_area_by_area WHERE admin_area_id = p_region_id;

  INSERT INTO coverage_unit_area_by_area (admin_area_id, unit_id, area_in_area_m2)
  SELECT
    r.admin_area_id,
    cu.unit_id,
    ST_Area(ST_Intersection(ST_Force2D(cu.geom), ST_Force2D(r.geom))::geography) AS area_m2
  FROM admin_area r
  JOIN admin_area c
    ON c.admin_area_id = r.parent_admin_area_id AND c.level = 1
  JOIN coverage_unit cu
    ON cu.admin_area_id = c.admin_area_id
  WHERE r.admin_area_id = p_region_id
    AND r.geom IS NOT NULL
    AND ST_Intersects(ST_Force2D(cu.geom), ST_Force2D(r.geom))
    AND ST_Area(ST_Intersection(ST_Force2D(cu.geom), ST_Force2D(r.geom))::geography) > 0;
END;
$$;

-- Function to populate cache for all regions.
CREATE OR REPLACE FUNCTION populate_all_region_unit_areas()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  r RECORD;
BEGIN
  FOR r IN SELECT admin_area_id FROM admin_area WHERE level = 2 LOOP
    PERFORM populate_region_unit_areas(r.admin_area_id);
  END LOOP;
END;
$$;

-- Views for totals and percents.

-- Total available coverage per area.
CREATE OR REPLACE VIEW v_area_total_m2 AS
SELECT a.admin_area_id,
       CASE
         WHEN a.level = 1 THEN COALESCE(SUM(cu.area_m2), 0)
         ELSE COALESCE(SUM(x.area_in_area_m2), 0)
       END AS coverage_total_area_m2
FROM admin_area a
LEFT JOIN coverage_unit cu
  ON a.level = 1 AND cu.admin_area_id = a.admin_area_id
LEFT JOIN coverage_unit_area_by_area x
  ON a.level = 2 AND x.admin_area_id = a.admin_area_id
GROUP BY a.admin_area_id;

-- Per-user traveled area per area.
CREATE OR REPLACE VIEW v_user_area_traveled_m2 AS
WITH uu AS (
  SELECT username, unit_id FROM user_traveled_unit
)
SELECT a.admin_area_id,
       uu.username,
       CASE
         WHEN a.level = 1 THEN COALESCE(SUM(cu.area_m2), 0)
         ELSE COALESCE(SUM(x.area_in_area_m2), 0)
       END AS traveled_area_m2
FROM admin_area a
JOIN uu ON TRUE
LEFT JOIN coverage_unit cu
  ON a.level = 1 AND cu.admin_area_id = a.admin_area_id AND cu.unit_id = uu.unit_id
LEFT JOIN coverage_unit_area_by_area x
  ON a.level = 2 AND x.admin_area_id = a.admin_area_id AND x.unit_id = uu.unit_id
GROUP BY a.admin_area_id, uu.username;

-- Completion percent per user and area.
CREATE OR REPLACE VIEW v_user_area_percent AS
SELECT t.admin_area_id,
       u.username,
       u.traveled_area_m2,
       t.coverage_total_area_m2,
       CASE
         WHEN t.coverage_total_area_m2 <= 0 THEN 0
         ELSE LEAST(100, CEIL((u.traveled_area_m2 / t.coverage_total_area_m2) * 100.0))::SMALLINT
       END AS percent
FROM v_area_total_m2 t
JOIN v_user_area_traveled_m2 u USING (admin_area_id);

COMMIT;
