-- Small-storage mode: raw tables move from daily to hourly partitions so retention can drop
-- them hour by hour (RAW_RETENTION_HOURS, default 24). Long-lived history is kept in compact
-- tables (Chainlink reports, markets, lifecycles, and from 0004 features and predictions).
--
-- The existing daily partitions hold only raw events, which this retention policy discards
-- anyway; they are dropped here because hourly ranges cannot be attached over them.
DO $$
DECLARE child regclass;
BEGIN
    FOR child IN
        SELECT i.inhrelid::regclass FROM pg_inherits i
        WHERE i.inhparent IN ('exchange_events'::regclass, 'orderbook_snapshots'::regclass)
    LOOP
        EXECUTE format('DROP TABLE %s', child);
    END LOOP;
END $$;

DROP FUNCTION ensure_daily_partitions(TEXT, DATE, INTEGER);

-- Creates `hours` hourly partitions starting at the UTC hour containing from_ts (idempotent).
CREATE FUNCTION ensure_hourly_partitions(parent TEXT, from_ts TIMESTAMPTZ, hours INTEGER) RETURNS void LANGUAGE plpgsql AS $$
DECLARE h TIMESTAMP;
BEGIN
    FOR i IN 0 .. hours - 1 LOOP
        h := date_trunc('hour', from_ts AT TIME ZONE 'UTC') + make_interval(hours => i);
        EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                       parent || '_p' || to_char(h, 'YYYYMMDDHH24'), parent,
                       h AT TIME ZONE 'UTC', (h + interval '1 hour') AT TIME ZONE 'UTC');
    END LOOP;
END $$;

-- Partitions of the raw tables with their upper bound, oldest first.
CREATE FUNCTION raw_partitions() RETURNS TABLE (parent TEXT, partition TEXT, upper_bound TIMESTAMPTZ) LANGUAGE sql STABLE AS $$
    SELECT p.relname::text, c.relname::text,
           substring(pg_get_expr(c.relpartbound, c.oid) FROM 'TO \(''([^'']+)''\)')::timestamptz
    FROM pg_inherits i
    JOIN pg_class c ON c.oid = i.inhrelid
    JOIN pg_class p ON p.oid = i.inhparent
    WHERE p.relname IN ('exchange_events', 'orderbook_snapshots')
    ORDER BY 3, 2
$$;
