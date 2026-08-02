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


------------------------------------Indexes for better performance-----------------------------

CREATE INDEX idx_vessel_time
ON vessel_positions(vessel_time DESC);

CREATE INDEX idx_vessel_mmsi_time
ON vessel_positions(mmsi, vessel_time DESC);

----------------------------------view for the visualisation-----------------------------------
CREATE OR REPLACE VIEW ais_last_hour AS
SELECT
    mmsi,
    MAX(ship_name) AS ship_name,
    ST_MakeLine(geom ORDER BY vessel_time) AS geom
FROM vessel_positions
WHERE vessel_time >= NOW() - INTERVAL '1 hour'
GROUP BY mmsi
HAVING COUNT(*) > 1;


CREATE OR REPLACE VIEW public.ais_live_vessels
AS SELECT DISTINCT ON (a.mmsi) a.id,
    a.mmsi,
    a.ship_name,
    a.region_source,
    a.geom,
    a.vessel_time,
    a.recorded_at,
    a.speed_sog,
    a.course_cog,
    a.true_heading,
    a.nav_status,
    a.nav_status_desc,
        CASE
            WHEN b.mmsi IS NOT NULL THEN 'High Speed'::text
            ELSE 'Normal Speed'::text
        END AS speed
   FROM vessel_positions a
     LEFT JOIN vessel_prediction b ON a.mmsi = b.mmsi
  ORDER BY a.mmsi, a.vessel_time DESC;


--------------------------------------fast moving vessels---------------------------------------------

-- public.vessel_prediction source

CREATE OR REPLACE VIEW public.vessel_prediction
AS WITH vessel_tracks AS (
         SELECT vessel_positions.mmsi,
            max(vessel_positions.ship_name::text)::character varying(250) AS ship_name,
            st_startpoint(st_makeline(vessel_positions.geom ORDER BY vessel_positions.vessel_time)) AS start_geom,
            st_endpoint(st_makeline(vessel_positions.geom ORDER BY vessel_positions.vessel_time)) AS end_geom,
            min(vessel_positions.vessel_time) AS start_time,
            max(vessel_positions.vessel_time) AS end_time,
            count(*) AS point_count
           FROM vessel_positions
          WHERE vessel_positions.vessel_time >= (now() - '00:15:00'::interval)
          GROUP BY vessel_positions.mmsi
         HAVING count(*) >= 5
        ), movement_stats AS (
         SELECT vessel_tracks.mmsi,
            vessel_tracks.ship_name,
            vessel_tracks.end_geom AS geom,
            st_distance(vessel_tracks.start_geom::geography, vessel_tracks.end_geom::geography) AS distance_m,
            EXTRACT(epoch FROM vessel_tracks.end_time - vessel_tracks.start_time) AS elapsed_sec,
            st_azimuth(vessel_tracks.start_geom, vessel_tracks.end_geom) AS heading_rad
           FROM vessel_tracks
        ), ranked AS (
         SELECT movement_stats.mmsi,
            movement_stats.ship_name,
            movement_stats.geom,
            movement_stats.distance_m,
            movement_stats.elapsed_sec,
            movement_stats.heading_rad,
            movement_stats.distance_m / NULLIF(movement_stats.elapsed_sec, 0::numeric)::double precision AS speed_mps
           FROM movement_stats
        )
 SELECT mmsi,
    ship_name,
    distance_m,
    elapsed_sec,
    round(speed_mps::numeric, 2) AS speed_mps,
    round((speed_mps * 1.94384::double precision)::numeric, 2) AS speed_knots,
    st_makeline(geom, st_project(geom::geography, speed_mps * 900::double precision, heading_rad)::geometry) AS geom
   FROM ranked
  WHERE distance_m > 1000::double precision AND speed_mps > 2::double precision
  ORDER BY (round(speed_mps::numeric, 2)) DESC
 LIMIT 10;