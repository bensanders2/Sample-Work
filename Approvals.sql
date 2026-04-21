/* ============================================================
   FILTERED CHANGES CTE
   Pre-filters pull requests to only include records that meet
   the baseline criteria before the main SELECT runs
   ============================================================ */
WITH filtered_changes AS (
    SELECT DISTINCT
        pr_id,
        author_id,
        description,
        reason_for_review,
        repository_name,
        scheduled_start_date_time,
        scheduled_end_date_time,
        environment,
        impact,
        submit_date_time,
        author,
        author_support_group,
        author_support_organization,
        review_manager,
        review_manager_id,
        status,
        template_name,
        is_fork,
        awaiting_approval

    FROM github_demo.repository_management.pull_request_public AS pr
    WHERE
        pr.status IN ('Finished')                                              -- Only completed PRs
        AND pr.submit_date_time > '2024-01-01'                                  -- Submitted after Jan 1 2024
        AND pr.is_fork = 0                                                      -- Exclude forked repositories
        AND pr.impact IN ('Medium', 'High')          -- Only medium/high impact
        AND pr.author_support_organization IN ('GitHub_XYZ', 'BS_GitHubWorkers', 'GitHubWorkers')  -- Specific orgs only
        AND pr.author_support_group <> ('Clientops_ABC_')                   -- Exclude this support group
        AND pr.change_class = 'Normal'                                          -- Normal change class only
        AND pr.awaiting_approval = 'Yes'                                          -- Must be waiting for approval
)

/* ============================================================
   MAIN SELECT
   Retrieves and transforms PR data, joining supporting tables
   for work documents, tasks, and related tickets
   ============================================================ */
SELECT
    /* -- Basic PR identifiers and status fields -- */
    pr.pr_id AS "PR ID",
    pr.impact AS "Impact",
    pr.status AS "Status",
    pr.awaiting_approval AS "Awaiting Approval",

    /* -- Format datetime fields into readable MM/DD/YYYY 12hr format -- */
    TO_VARCHAR(pr.scheduled_start_date_time, 'MM/DD/YYYY HH12:MI:SS AM') AS "Scheduled Start Date",
    TO_VARCHAR(pr.scheduled_end_date_time,   'MM/DD/YYYY HH12:MI:SS AM') AS "Scheduled End Date",

    /* -- Use author ID if present, otherwise fall back to review manager ID -- */
    (CASE
        WHEN (pr.author_id <> 'NS' OR pr.author_id <> null) THEN pr.author_id
        ELSE pr.review_manager_id
    END) AS "Author ID",

    /* -- Retrieve manager account from HR worker table via author ID -- */
    (CASE
        WHEN (pr.author_id <> 'NS' OR pr.author_id <> null) THEN manager.account
    END) AS "Manager ID",

    /* -- Check if a description/notes field has meaningful content (>10 chars) -- */
    (CASE
        WHEN LENGTH(pr.description) > 10 THEN 'PRESENT'
        ELSE 'MISSING'
    END) AS "Notes",

    /* -- Support group and org pulled directly from the PR record -- */
    pr.author_support_group AS "Support Group",
    pr.author_support_organization AS "Support Org",

    /* -- Check if a reason for Review has been provided -- */
    (CASE WHEN pr.reason_for_review <> 'NS'
        THEN 'PRESENT'
        ELSE 'MISSING'
    END) AS "Change Review",

    /* -- Check if a repository name has been provided -- */
    (CASE WHEN pr.repository_name <> 'NS'
        THEN 'PRESENT'
        ELSE 'MISSING'
    END) AS "Repository",

    /* -------------------------------------------------------
       WORK DOCUMENTS CHECKS (from pr_work_documents)
       Each block checks whether a specific document type is
       present, either via notes text (>20 chars) or an
       attachment. MAX() is used because of the GROUP BY —
       it ensures 'PRESENT' wins over 'MISSING' across rows
    ------------------------------------------------------- */

    /* -- Work Instruction: notes or attachment must be present -- */
    MAX(CASE WHEN
        (info.work_info_type = 'Work Instruction'
            AND LENGTH(info.notes) > 20)
        OR (info.work_info_type = 'Work Instruction' AND
            (info.attachment_1 <> 'NS'
            OR info.attachment_2 <> 'NS'
            OR info.attachment_3 <> 'NS'))
        THEN 'PRESENT'
        ELSE 'MISSING'
    END) AS "Work Instruction",

    /* -- Backout Steps: notes or attachment must be present -- */
    MAX(CASE WHEN
        (info.work_info_type = 'Backout Steps'
            AND LENGTH(info.notes) > 20)
        OR (info.work_info_type = 'Backout Steps' AND
            (info.attachment_1 <> 'NS'
            OR info.attachment_2 <> 'NS'
            OR info.attachment_3 <> 'NS'))
        THEN 'PRESENT'
        ELSE 'MISSING'
    END) AS "Backout Steps",


    /* -- Client Approval: must be approved by Client Reviewer.
          Moderate/Limited impact PRs do not require this -- */
    MAX(CASE WHEN
        (info.work_info_type = 'Client Approval'
            AND info.notes LIKE 'Approved by Client Reviewer%') THEN 'PRESENT'
        WHEN pr.impact = 'Medium' THEN 'NOT REQUIRED'
        ELSE 'MISSING'
    END) AS "Client Approval",

    /* -- Proof of Completion: must have meaningful notes
          (excluding the default placeholder text) or an attachment -- */
    MAX(CASE
        WHEN (info.work_info_type = 'Proof of Completion'
            AND (info.notes IS NOT NULL OR LENGTH(info.notes) > 0)
            AND info.notes <> 'Example Default Language When Creating Proof of Completion')
        OR (info.work_info_type = 'Proof of Completion' AND
            (info.attachment_1 <> 'NS'
            OR info.attachment_2 <> 'NS'
            OR info.attachment_3 <> 'NS'))
        THEN 'PRESENT'
        ELSE 'MISSING'
    END) AS "Proof of Completion",

    /* -------------------------------------------------------
       TASK CHECKS (from pr_work_task)
       Each block checks whether a specific review task was
       completed successfully by someone other than the author
    ------------------------------------------------------- */

    /* -- Team Peer Review: closed with Success by a different assignee than ticket owner -- */
    MAX(CASE WHEN
        tsk.task_name LIKE '%Team Review'
        AND tsk.status = 'Closed'
        AND tsk.status_reason = 'Success'
        AND tsk.assignee_id <> pr.author_id
        THEN 'PRESENT'
        ELSE 'MISSING'
    END) AS "Team Review",

    /* -- Senior Team Review: same as above but for senior-level review -- */
    MAX(CASE WHEN
        tsk.task_name LIKE 'Senior Team Review'
        AND tsk.status = 'Closed'
        AND tsk.status_reason = 'Success'
        AND tsk.assignee_id <> pr.author_id
        THEN 'PRESENT'
        ELSE 'MISSING'
    END) AS "Senior Team Review",

    /* -- Co-Driver Review: closed by a different assignee.
          Not required for Medium impact PRs -- */
    MAX(CASE WHEN
        (tsk.task_name LIKE '%co-driver'
        AND tsk.status = 'Closed'
        AND tsk.assignee_id <> pr.author_id)
        THEN 'PRESENT'
        WHEN (pr.impact = 'Medium') THEN 'NOT REQUIRED'
        ELSE 'MISSING'
    END) AS "Co-Driver",

    /* -- Check if an environment has been specified -- */
    (CASE WHEN pr.environment <> 'NS'
        THEN 'PRESENT'
        ELSE 'MISSING'
    END) AS "Location"

/* ============================================================
   JOINS
   Bring in HR worker data for author/manager name resolution,
   and the three supporting PR tables for work documents, related tickets and tasks
   ============================================================ */
FROM filtered_changes AS pr
    /* -- Resolve author name from HR worker table -- */
    LEFT JOIN HR.WORKERS worker ON upper(pr.author_id) = upper(worker.account)
    /* -- Resolve manager name by linking worker to their manager -- */
    LEFT JOIN HR.WORKERS manager ON worker.manager_worker_id = manager.worker_id
    /* -- PR relationship data (e.g. linked software components) -- */
    LEFT JOIN
        github_demo.repository_management.pr_related_tickets AS rln
        ON pr.pr_id = rln.pr_id
    /* -- Work documents (work instruction, backout steps, completion proof) -- */
    LEFT JOIN
        github_demo.repository_management.pr_work_documents AS info
        ON pr.pr_id = info.pr_id
    /* -- Tasks associated with the PR (team reviews, co-driver) -- */
    LEFT JOIN
        github_demo.repository_management.pr_work_task AS tsk
        ON pr.pr_id = tsk.parent_id

/* ============================================================
   FILTERS
   Limit results to PRs linked to Software Component via
   a 'Direct' relationship type
   ============================================================ */
WHERE
    rln.request_type = 'Software Component'
    AND rln.relationship_type = 'Direct'

/* ============================================================
   GROUP BY
   Required because of MAX() aggregations on work info and
   task fields — groups by all non-aggregated columns
   ============================================================ */
GROUP BY
    pr.pr_id,
    pr.description,
    pr.reason_for_review,
    pr.repository_name,
    pr.scheduled_start_date_time,
    pr.environment,
    rln.relationship_type,
    rln.request_type,
    pr.impact,
    pr.scheduled_end_date_time,
    pr.submit_date_time,
    pr.author,
    pr.author_support_group,
    pr.author_support_organization,
    pr.review_manager,
    pr.status,
    pr.template_name,
    pr.is_fork,
    pr.author_id,
    pr.review_manager_id,
    manager.account
