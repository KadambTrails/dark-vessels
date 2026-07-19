-- 1. Ensure PostGIS spatial tools are active
CREATE EXTENSION IF NOT EXISTS postgis;

-- 2. the tracking configuration matrix
CREATE TABLE IF NOT EXISTS monitoring_regions (
    id SERIAL PRIMARY KEY,
    region_name VARCHAR(100) UNIQUE NOT NULL,
    geom GEOMETRY(Polygon, 4326) NOT NULL, -- Coordinates stored in WGS84 (Lng/Lat)
    is_active BOOLEAN DEFAULT TRUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. Create a spatial index for lightning-fast lookups
CREATE INDEX IF NOT EXISTS idx_monitoring_regions_geom ON monitoring_regions USING GIST (geom);

-- 4. Seed with true boundaries (Using WKT POLYGON: Lng Lat)
INSERT INTO monitoring_regions (region_name, geom, is_active)
VALUES
(
    'Great_Barrier_Reef_Marine_Park',
    ST_GeomFromText('POLYGON((145.0 -24.0, 155.0 -24.0, 155.0 -10.0, 145.0 -10.0, 145.0 -24.0))', 4326),
    TRUE
),
(
    'Galapagos_Marine_Reserve',
    ST_GeomFromText('POLYGON((-93.0 -2.0, -89.0 -2.0, -89.0 2.0, -93.0 2.0, -93.0 -2.0))', 4326),
    TRUE
),
(
    'Papahanaumokuakea_Marine_Monument',
    ST_GeomFromText('POLYGON((-179.0 21.0, -160.0 21.0, -160.0 30.0, -179.0 30.0, -179.0 21.0))', 4326),
    TRUE
),
(
    'Chagos_Marine_Protected_Area',
    ST_GeomFromText('POLYGON((70.0 -8.5, 74.5 -8.5, 74.5 -3.0, 70.0 -3.0, 70.0 -8.5))', 4326),
    TRUE
),
(
    'Phoenix_Islands_Protected_Area',
    ST_GeomFromText('POLYGON((-175.0 -7.0, -168.0 -7.0, -168.0 5.0, -175.0 5.0, -175.0 -7.0))', 4326),
    TRUE
)
ON CONFLICT (region_name)
DO UPDATE
SET geom = EXCLUDED.geom,
    is_active = EXCLUDED.is_active;