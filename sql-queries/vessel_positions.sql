CREATE TABLE IF NOT EXISTS vessel_positions (
    id BIGSERIAL PRIMARY KEY,
    mmsi INT NOT NULL,
    ship_name VARCHAR(250),
    region_source VARCHAR(100),
    geom GEOMETRY(Point, 4326) NOT NULL,
    vessel_time timestamp WITH TIME zone,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_vessel_positions_geom ON vessel_positions USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_vessel_positions_mmsi ON vessel_positions(mmsi);