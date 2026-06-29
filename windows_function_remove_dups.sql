-- ============================================================
-- DEDUPLICATION PATTERN: Hourly Incident Scan Table
-- Keeps only the most recent scan record per ticket
-- ============================================================

WITH ranked_incidents AS (

    SELECT
        incident_id,
        ticket_number,
        status,
        priority,
        assigned_group,
        summary,
        last_modified_dttm,
        scan_dttm,          -- timestamp of when the hourly job pulled this record

        -- Partition by the natural key (ticket number), order by newest scan first.
        -- This assigns rn=1 to the most recent scan for each ticket.
        ROW_NUMBER() OVER (
            PARTITION BY ticket_number
            ORDER BY scan_dttm DESC
        ) AS rn

    FROM db.schema.incident_scan_stage   -- your raw hourly-load staging table

)

SELECT
    incident_id,
    ticket_number,
    status,
    priority,
    assigned_group,
    summary,
    last_modified_dttm,
    scan_dttm
FROM ranked_incidents
WHERE rn = 1   -- only the latest scan per ticket survives
ORDER BY ticket_number
;
