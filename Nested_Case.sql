-- ============================================================
-- Metric: Service Request Fulfillment Compliance Flag
-- Purpose: Flags whether a service request was fulfilled with
--          proper documentation based on request type and status.
-- Context: Excludes catalog/standard requests; evaluates only
--          complex or urgent requests in terminal states.
-- ============================================================

CASE
  -- Exclude routine catalog requests from compliance scoring
  WHEN "Service Mgmt"."Request Dim"."Request Category" = 'Catalog'
    THEN NULL

  -- Evaluate complex and urgent requests only
  WHEN "Service Mgmt"."Request Dim"."Request Category" IN ('Complex', 'Urgent')
    THEN
      CASE
        -- Only score requests that have reached a terminal state
        WHEN "Service Mgmt"."Request Dim"."Status" IN ('Resolved', 'Closed')
          THEN
            CASE
              WHEN
                -- Check 1: Fulfillment evidence (attachment or valid notes)
                (
                  "Request Compliance Details"."Fulfillment Attachment" IS NOT NULL
                  OR (
                    "Request Compliance Details"."Fulfillment Notes" IS NOT NULL
                    AND "Request Compliance Details"."Fulfillment Notes" <>
                      'Attach evidence confirming the request was fulfilled per acceptance criteria.'
                  )
                )
                -- Check 2: Closure verification (attachment or valid notes)
                AND (
                  "Request Compliance Details"."Closure Review Attachment" IS NOT NULL
                  OR (
                    "Request Compliance Details"."Closure Review Notes" IS NOT NULL
                    AND "Request Compliance Details"."Closure Review Notes" <>
                      'Attach evidence confirming the request was fulfilled per acceptance criteria.'
                  )
                )
              THEN 1   -- Compliant
              ELSE 0   -- Non-compliant
            END
        ELSE NULL  -- Not yet in a scoreable state
      END
  ELSE NULL  -- All other request categories excluded
END
