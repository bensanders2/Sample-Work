/*
  ============================================================
  Demo Query: Hardened Free-Text Version Parsing
  from ITSM Deployment Tickets
  ============================================================

  BACKGROUND & PROBLEM
  --------------------
  Deployment tickets contain a freeform Description field where
  engineers are expected to document which software version is
  being upgraded and what the target version is.

  An initial query was built assuming a consistent format:
    "AppStack [from_version] to [to_version]"

  However, after running that query against real data, a
  significant number of tickets returned NULL — meaning the
  version fields could not be parsed. Investigation of the raw
  ticket text revealed that engineers were filling out the field
  in at least two distinct phrase styles, with multiple separator
  variations each:

    Style A — "AppStack X.X to Y.Y"       (original expected format)
    Style B — "Upgrade from X.X to Y.Y"   (discovered in production)

  Within each style, separators between versions varied:
    - " to "       (standard)
    - " - "        (dash instead of "to")
    - whitespace   (no separator word at all)

  This query is the hardened, iterated version that accounts for
  all discovered variations using a COALESCE stack of REGEXP_SUBSTR
  patterns ordered from most specific to most permissive.

  TECHNIQUES DEMONSTRATED
  -----------------------
  - Multi-pattern COALESCE for resilient free-text extraction
  - REGEXP_SUBSTR with capture groups (group 1 = from, group 2 = to)
  - Dual phrase anchor coverage (two naming conventions)
  - Separator variation handling (" to ", " - ", whitespace-only)
  - CASE/ILIKE for fuzzy version band → master record mapping
  - ELSE NULL with downstream classification flag for data quality
  - CTE staging for readability and reuse
  ============================================================
*/

WITH deployment_tickets AS (
  /*
    Stage 1: Filter to relevant tickets only.
    Scoping to a specific template name keeps the regex logic
    focused on a known description format and avoids false
    matches from unrelated ticket types.
  */
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
    template_name IN (
      'AppStack Upgrade Scheduled Maintenance',
      'AppStack Upgrade Continuous Availability'
    )
    AND submitted_at >= '2025-01-01'
),

parsed AS (
  /*
    Stage 2: Extract structured version fields from free text.

    Each COALESCE block below tries patterns in order of specificity.
    The first pattern that returns a non-NULL result wins.
    If all patterns fail, the field returns NULL — which is intentional
    so those rows can be identified and reviewed rather than silently lost.

    Pattern inventory (applied consistently across Version_Range,
    From_Version, To_Version, and the Master_CRQ CASE block):

      1. 'AppStack ([0-9.]+) to ([0-9.]+)'     -- Style A, "to" separator
      2. 'AppStack ([0-9.]+) - ([0-9.]+)'      -- Style A, dash separator
      3. 'AppStack\s*([0-9.]+)\s*to\s*([0-9.]+)' -- Style A, variable whitespace
      4. 'AppStack\s*([0-9.]+)\s+([0-9.]+)'   -- Style A, space only (no separator word)
      5. 'Upgrade from ([0-9.]+) - ([0-9.]+)' -- Style B, dash separator
      6. 'Upgrade from ([0-9.]+) to ([0-9.]+)' -- Style B, "to" separator
      7. 'Upgrade from ([0-9.]+) ([0-9.]+)'   -- Style B, space only

    Group 1 = source (from) version
    Group 2 = destination (to) version
  */
  SELECT
    ticket_id,
    description,
    submitted_at,
    assignee_group,
    environment,
    status,
    template_name,
    is_emergency,

    -- First word of description, used as a short display label
    SPLIT_PART(description, ' ', 1) AS mnemonic,

    -- ------------------------------------------------------------------
    -- VERSION_RANGE
    -- The full matched range string (e.g. "10.1 to 2025.2") captured
    -- from group 1. Used for display and grouping. Tries each phrase
    -- anchor and separator style before returning NULL.
    -- ------------------------------------------------------------------
    COALESCE(
      REGEXP_SUBSTR(description, 'AppStack ([0-9.]+) to ([0-9.]+)',           1, 1, 'e', 1),
      REGEXP_SUBSTR(description, 'AppStack ([0-9.]+) - ([0-9.]+)',            1, 1, 'e', 1),
      REGEXP_SUBSTR(description, 'AppStack\\s*([0-9.]+)\\s*to\\s*([0-9.]+)', 1, 1, 'e', 1),
      REGEXP_SUBSTR(description, 'AppStack\\s*([0-9.]+)\\s+([0-9.]+)',       1, 1, 'e', 1),
      REGEXP_SUBSTR(description, 'Upgrade from ([0-9.]+) - ([0-9.]+)',        1, 1, 'e', 1),
      REGEXP_SUBSTR(description, 'Upgrade from ([0-9.]+) to ([0-9.]+)',       1, 1, 'e', 1),
      REGEXP_SUBSTR(description, 'Upgrade from ([0-9.]+) ([0-9.]+)',          1, 1, 'e', 1)
    ) AS version_range,

    -- ------------------------------------------------------------------
    -- FROM_VERSION
    -- The source version — capture group 1 from whichever pattern fires.
    -- This is the version the system is upgrading FROM.
    -- ------------------------------------------------------------------
    COALESCE(
      REGEXP_SUBSTR(description, 'AppStack ([0-9.]+) to ([0-9.]+)',           1, 1, 'e', 1),
      REGEXP_SUBSTR(description, 'AppStack ([0-9.]+) - ([0-9.]+)',            1, 1, 'e', 1),
      REGEXP_SUBSTR(description, 'AppStack\\s*([0-9.]+)\\s*to\\s*([0-9.]+)', 1, 1, 'e', 1),
      REGEXP_SUBSTR(description, 'AppStack\\s*([0-9.]+)\\s+([0-9.]+)',       1, 1, 'e', 1),
      REGEXP_SUBSTR(description, 'Upgrade from ([0-9.]+) - ([0-9.]+)',        1, 1, 'e', 1),
      REGEXP_SUBSTR(description, 'Upgrade from ([0-9.]+) to ([0-9.]+)',       1, 1, 'e', 1),
      REGEXP_SUBSTR(description, 'Upgrade from ([0-9.]+) ([0-9.]+)',          1, 1, 'e', 1)
    ) AS from_version,

    -- ------------------------------------------------------------------
    -- TO_VERSION
    -- The destination version — capture group 2 from whichever pattern
    -- fires. Identical pattern stack as From_Version; only the final
    -- argument changes from 1 → 2 to pull the second capture group.
    -- This is the version the system is upgrading TO.
    -- ------------------------------------------------------------------
    COALESCE(
      REGEXP_SUBSTR(description, 'AppStack ([0-9.]+) to ([0-9.]+)',           1, 1, 'e', 2),
      REGEXP_SUBSTR(description, 'AppStack ([0-9.]+) - ([0-9.]+)',            1, 1, 'e', 2),
      REGEXP_SUBSTR(description, 'AppStack\\s*([0-9.]+)\\s*to\\s*([0-9.]+)', 1, 1, 'e', 2),
      REGEXP_SUBSTR(description, 'AppStack\\s*([0-9.]+)\\s+([0-9.]+)',       1, 1, 'e', 2),
      REGEXP_SUBSTR(description, 'Upgrade from ([0-9.]+) - ([0-9.]+)',        1, 1, 'e', 2),
      REGEXP_SUBSTR(description, 'Upgrade from ([0-9.]+) to ([0-9.]+)',       1, 1, 'e', 2),
      REGEXP_SUBSTR(description, 'Upgrade from ([0-9.]+) ([0-9.]+)',          1, 1, 'e', 2)
    ) AS to_version,

    -- ------------------------------------------------------------------
    -- MASTER_CRQ
    -- Maps the extracted To_Version to a canonical parent change record.
    -- The same 7-pattern COALESCE extracts To_Version inline here so
    -- the CASE can evaluate it directly without a subquery.
    --
    -- ILIKE '2025.1%' handles minor version suffix variations
    -- (e.g. "2025.1A", "2025.1x", "2025.10") that engineers appended
    -- inconsistently — the wildcard absorbs all of them into one band.
    --
    -- ELSE NULL is intentional: unmatched tickets surface as
    -- UNCLASSIFIED in the final output for data quality review.
    -- ------------------------------------------------------------------
    CASE
      WHEN COALESCE(
        REGEXP_SUBSTR(description, 'AppStack ([0-9.]+) to ([0-9.]+)',           1, 1, 'e', 2),
        REGEXP_SUBSTR(description, 'AppStack ([0-9.]+) - ([0-9.]+)',            1, 1, 'e', 2),
        REGEXP_SUBSTR(description, 'AppStack\\s*([0-9.]+)\\s*to\\s*([0-9.]+)', 1, 1, 'e', 2),
        REGEXP_SUBSTR(description, 'AppStack\\s*([0-9.]+)\\s+([0-9.]+)',       1, 1, 'e', 2),
        REGEXP_SUBSTR(description, 'Upgrade from ([0-9.]+) - ([0-9.]+)',        1, 1, 'e', 2),
        REGEXP_SUBSTR(description, 'Upgrade from ([0-9.]+) to ([0-9.]+)',       1, 1, 'e', 2),
        REGEXP_SUBSTR(description, 'Upgrade from ([0-9.]+) ([0-9.]+)',          1, 1, 'e', 2)
      ) ILIKE '2025.1%' THEN 'CRQ-MASTER-Q1-2025'

      WHEN COALESCE(
        REGEXP_SUBSTR(description, 'AppStack ([0-9.]+) to ([0-9.]+)',           1, 1, 'e', 2),
        REGEXP_SUBSTR(description, 'AppStack ([0-9.]+) - ([0-9.]+)',            1, 1, 'e', 2),
        REGEXP_SUBSTR(description, 'AppStack\\s*([0-9.]+)\\s*to\\s*([0-9.]+)', 1, 1, 'e', 2),
        REGEXP_SUBSTR(description, 'AppStack\\s*([0-9.]+)\\s+([0-9.]+)',       1, 1, 'e', 2),
        REGEXP_SUBSTR(description, 'Upgrade from ([0-9.]+) - ([0-9.]+)',        1, 1, 'e', 2),
        REGEXP_SUBSTR(description, 'Upgrade from ([0-9.]+) to ([0-9.]+)',       1, 1, 'e', 2),
        REGEXP_SUBSTR(description, 'Upgrade from ([0-9.]+) ([0-9.]+)',          1, 1, 'e', 2)
      ) ILIKE '2025.2%' THEN 'CRQ-MASTER-Q2-2025'

      WHEN COALESCE(
        REGEXP_SUBSTR(description, 'AppStack ([0-9.]+) to ([0-9.]+)',           1, 1, 'e', 2),
        REGEXP_SUBSTR(description, 'AppStack ([0-9.]+) - ([0-9.]+)',            1, 1, 'e', 2),
        REGEXP_SUBSTR(description, 'AppStack\\s*([0-9.]+)\\s*to\\s*([0-9.]+)', 1, 1, 'e', 2),
        REGEXP_SUBSTR(description, 'AppStack\\s*([0-9.]+)\\s+([0-9.]+)',       1, 1, 'e', 2),
        REGEXP_SUBSTR(description, 'Upgrade from ([0-9.]+) - ([0-9.]+)',        1, 1, 'e', 2),
        REGEXP_SUBSTR(description, 'Upgrade from ([0-9.]+) to ([0-9.]+)',       1, 1, 'e', 2),
        REGEXP_SUBSTR(description, 'Upgrade from ([0-9.]+) ([0-9.]+)',          1, 1, 'e', 2)
      ) ILIKE '2025.3%' THEN 'CRQ-MASTER-Q3-2025'

      WHEN COALESCE(
        REGEXP_SUBSTR(description, 'AppStack ([0-9.]+) to ([0-9.]+)',           1, 1, 'e', 2),
        REGEXP_SUBSTR(description, 'AppStack ([0-9.]+) - ([0-9.]+)',            1, 1, 'e', 2),
        REGEXP_SUBSTR(description, 'AppStack\\s*([0-9.]+)\\s*to\\s*([0-9.]+)', 1, 1, 'e', 2),
        REGEXP_SUBSTR(description, 'AppStack\\s*([0-9.]+)\\s+([0-9.]+)',       1, 1, 'e', 2),
        REGEXP_SUBSTR(description, 'Upgrade from ([0-9.]+) - ([0-9.]+)',        1, 1, 'e', 2),
        REGEXP_SUBSTR(description, 'Upgrade from ([0-9.]+) to ([0-9.]+)',       1, 1, 'e', 2),
        REGEXP_SUBSTR(description, 'Upgrade from ([0-9.]+) ([0-9.]+)',          1, 1, 'e', 2)
      ) ILIKE '2025.4%' THEN 'CRQ-MASTER-Q4-2025'

      ELSE NULL
    END AS master_crq

  FROM deployment_tickets
)

-- ------------------------------------------------------------------
-- Final output
-- All parsed fields surfaced for reporting and audit.
-- classification_status makes data quality issues immediately visible —
-- UNCLASSIFIED rows indicate tickets where no pattern fired, which
-- should trigger a review of whether a new phrase style has appeared.
-- ------------------------------------------------------------------
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
  CASE
    WHEN master_crq IS NULL THEN 'UNCLASSIFIED'
    ELSE 'MATCHED'
  END AS classification_status
FROM parsed
ORDER BY
  master_crq NULLS LAST,
  submitted_at;
