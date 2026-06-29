/*
  Demo Query: Free-Text Version Parsing from ITSM Deployment Tickets
  ------------------------------------------------------------------
  Context:
    Engineers submit deployment tickets with a freeform Description field.
    When populated consistently, it follows the pattern:
      "AppStack [X.X]+ to [Y.Y]+ - <environment>"
    However, real-world data has variation in phrasing, spacing, and
    capitalization. This query extracts structured version metadata from
    that free text with maximum coverage across imperfect submissions.

  Techniques demonstrated:
    - REGEXP_SUBSTR with COALESCE fallback for resilient extraction
    - SPLIT_PART for positional token parsing
    - CASE/ILIKE for fuzzy pattern-to-record mapping (master CRQ linkage)
    - CTE-based staging for readability and reuse
*/

WITH deployment_tickets AS (
  SELECT
    ticket_id,
    description,
    submitted_at,
    assignee_group,
    environment,
    status,
    template_name,
    is_emergency

  FROM prod_db.itsm.deployment_tickets
  WHERE
    template_name = 'AppStack Upgrade Scheduled Maintenance'
    AND submitted_at >= '2025-01-01'
),

parsed AS (
  SELECT
    ticket_id,
    description,
    submitted_at,
    assignee_group,
    environment,
    status,
    template_name,
    is_emergency,

    -- ----------------------------------------------------------------
    -- Mnemonic: first word of the description, used as a short label
    -- ----------------------------------------------------------------
    SPLIT_PART(description, ' ', 1) AS mnemonic,

    -- ----------------------------------------------------------------
    -- Version range string: captures "X.X to Y.Y" for display/grouping
    -- Primary:  regex match on the canonical "AppStack [X.X]+ to [Y.Y]+" pattern
    -- Fallback: regex match on trailing version suffix if primary fails
    -- ----------------------------------------------------------------
    COALESCE(
      REGEXP_SUBSTR(
        description,
        'AppStack ([0-9]+\\.[0-9]+)+ to ([0-9]+\\.[0-9]+)+',
        1, 1, 'e', 1
      ),
      REGEXP_SUBSTR(description, 'AppStack (.+)$', 1, 1, 'e', 1)
    ) AS version_range,

    -- ----------------------------------------------------------------
    -- From_Version: token immediately before " to " in the range string
    -- Positional split after extracting the version range above
    -- ----------------------------------------------------------------
    SPLIT_PART(
      COALESCE(
        REGEXP_SUBSTR(
          description,
          'AppStack ([0-9]+\\.[0-9]+)+ to ([0-9]+\\.[0-9]+)+',
          1, 1, 'e', 1
        ),
        REGEXP_SUBSTR(description, 'AppStack (.+)$', 1, 1, 'e', 1)
      ),
      ' to ',
      1
    ) AS from_version,

    -- ----------------------------------------------------------------
    -- To_Version: token immediately after " to " in the range string
    -- ----------------------------------------------------------------
    SPLIT_PART(
      COALESCE(
        REGEXP_SUBSTR(
          description,
          'AppStack ([0-9]+\\.[0-9]+)+ to ([0-9]+\\.[0-9]+)+',
          1, 1, 'e', 1
        ),
        REGEXP_SUBSTR(description, 'AppStack (.+)$', 1, 1, 'e', 1)
      ),
      ' to ',
      2
    ) AS to_version,

    -- ----------------------------------------------------------------
    -- Master_CRQ: maps version upgrade bands to canonical parent CRQs.
    -- Engineers reference version ranges in freetext with slight variation
    -- (e.g., "2025.1x", "2025.1X", "v2025.1"), so ILIKE handles case
    -- insensitivity and wildcard coverage for each quarterly release band.
    -- ELSE NULL intentionally left open — unmatched tickets are flagged
    -- downstream rather than silently miscategorized.
    -- ----------------------------------------------------------------
    CASE
      WHEN SPLIT_PART(
        COALESCE(
          REGEXP_SUBSTR(description, 'AppStack ([0-9]+\\.[0-9]+)+ to ([0-9]+\\.[0-9]+)+', 1, 1, 'e', 1),
          REGEXP_SUBSTR(description, 'AppStack (.+)$', 1, 1, 'e', 1)
        ),
        ' to ', 2
      ) ILIKE '2025.1%' THEN 'CRQ-MASTER-Q1-2025'

      WHEN SPLIT_PART(
        COALESCE(
          REGEXP_SUBSTR(description, 'AppStack ([0-9]+\\.[0-9]+)+ to ([0-9]+\\.[0-9]+)+', 1, 1, 'e', 1),
          REGEXP_SUBSTR(description, 'AppStack (.+)$', 1, 1, 'e', 1)
        ),
        ' to ', 2
      ) ILIKE '2025.2%' THEN 'CRQ-MASTER-Q2-2025'

      WHEN SPLIT_PART(
        COALESCE(
          REGEXP_SUBSTR(description, 'AppStack ([0-9]+\\.[0-9]+)+ to ([0-9]+\\.[0-9]+)+', 1, 1, 'e', 1),
          REGEXP_SUBSTR(description, 'AppStack (.+)$', 1, 1, 'e', 1)
        ),
        ' to ', 2
      ) ILIKE '2025.3%' THEN 'CRQ-MASTER-Q3-2025'

      WHEN SPLIT_PART(
        COALESCE(
          REGEXP_SUBSTR(description, 'AppStack ([0-9]+\\.[0-9]+)+ to ([0-9]+\\.[0-9]+)+', 1, 1, 'e', 1),
          REGEXP_SUBSTR(description, 'AppStack (.+)$', 1, 1, 'e', 1)
        ),
        ' to ', 2
      ) ILIKE '2025.4%' THEN 'CRQ-MASTER-Q4-2025'

      ELSE NULL  -- Unmatched: out-of-band or malformed; handle downstream
    END AS master_crq

  FROM deployment_tickets
)

-- ----------------------------------------------------------------
-- Final output: surface all structured fields for reporting/audit.
-- Rows where master_crq IS NULL indicate tickets that couldn't be
-- classified — useful for data quality monitoring.
-- ----------------------------------------------------------------
SELECT
  ticket_id,
  master_crq,
  mnemonic,
  version_range,
  from_version,
  to_version,
  description,
  submitted_at,
  assignee_group,
  environment,
  status,
  template_name,
  is_emergency,
  CASE WHEN master_crq IS NULL THEN 'UNCLASSIFIED' ELSE 'MATCHED' END AS classification_status
FROM parsed
ORDER BY
  master_crq NULLS LAST,
  submitted_at;
