-- 1. Drop existing non-partitioned table
DROP TABLE IF EXISTS vessel_positions CASCADE;

-- 2. Create Parent Partitioned Table
CREATE TABLE vessel_positions (
    id BIGSERIAL,
    mmsi BIGINT NOT NULL,
    ship_name VARCHAR(250),
    region_source VARCHAR(100),
    geom GEOMETRY(Point, 4326) NOT NULL,
    vessel_time TIMESTAMP WITH TIME ZONE NOT NULL,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    speed_sog NUMERIC(5, 2),
    course_cog NUMERIC(5, 2),
    true_heading INT,
    nav_status INT,
    nav_status_desc VARCHAR(100),
    PRIMARY KEY (id, vessel_time) -- Primary key MUST include partition key
) PARTITION BY RANGE (vessel_time);

-- 3. Create global spatial index (automatically inherited by child partitions)
CREATE INDEX idx_vessel_positions_geom ON vessel_positions USING GIST(geom);
CREATE INDEX idx_vessel_positions_mmsi ON vessel_positions(mmsi);

CREATE OR REPLACE VIEW ais_last_hour AS
SELECT *
FROM vessel_positions
WHERE vessel_time >= NOW() - INTERVAL '1 hour';