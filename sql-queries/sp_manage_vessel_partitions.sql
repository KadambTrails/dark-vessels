CREATE OR REPLACE PROCEDURE sp_manage_vessel_partitions(
    p_retention_days INT DEFAULT 7,
    p_future_days INT DEFAULT 7
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_target_date DATE;
    v_partition_name TEXT;
    v_start_str TEXT;
    v_end_str TEXT;
    v_old_date DATE;
    v_old_partition TEXT;
BEGIN

    --1. CREATE FUTURE PARTITIONS
    
    FOR i IN 0..p_future_days LOOP
        v_target_date := CURRENT_DATE + i;
        v_partition_name := 'vessel_positions_' || to_char(v_target_date, 'YYYY_MM_DD');
        v_start_str := to_char(v_target_date, 'YYYY-MM-DD 00:00:00+00');
        v_end_str := to_char(v_target_date + 1, 'YYYY-MM-DD 00:00:00+00');

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF vessel_positions 
             FOR VALUES FROM (%L) TO (%L);',
            v_partition_name, v_start_str, v_end_str
        );
    END LOOP;

    -- 2. DROP EXPIRED PARTITIONS (Older than p_retention_days)

    FOR i IN 1..7 LOOP
        v_old_date := CURRENT_DATE - (p_retention_days + i);
        v_old_partition := 'vessel_positions_' || to_char(v_old_date, 'YYYY_MM_DD');

        EXECUTE format('DROP TABLE IF EXISTS %I CASCADE;', v_old_partition);
    END LOOP;

    RAISE NOTICE 'Partition maintenance complete. Retained past % days, prepared next % days.', 
                 p_retention_days, p_future_days;
END;
$$;