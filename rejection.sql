/* ============================================================
   REJECTED PULL REQUESTS REPORT
   Returns PRs with a 'Rejection' status, pulling in the most
   recent rejection notices for each, ordered by scheduled start date
   ============================================================ */
SELECT
    /* -- Basic PR identifiers and key metadata -- */
    pr.pr_id AS "PR ID",
    pr.impact AS "Impact",
    pr.summary AS "Summary",
    pr.author AS "Author",
    pr.author_id AS "Author ID",
    pr.author_support_group AS "Author Support Group",
    pr.author_support_organization AS "Author Support Org",

    /* -- Concatenate first and last name from worker directory for HR manager display -- */
    CONCAT(manager.first_name, ' ', manager.last_name) AS "HR Manager",
    manager.account AS "Manager ID",

    /* -- Format datetime fields into readable MM/DD/YYYY 12hr format -- */
    TO_VARCHAR(pr.scheduled_start_date_time, 'MM/DD/YYYY HH12:MI:SS AM') AS "Scheduled Start Date",
    TO_VARCHAR(pr.scheduled_end_date_time,   'MM/DD/YYYY HH12:MI:SS AM') AS "Scheduled End Date",

    /* -- Rejection notice pulled from the most recent rejection document -- */
    rej.notice AS "Rejection Notice"

/* ============================================================
   JOINS
   Bring in worker directory for author/manager resolution,
   and a subquery to get the latest rejection document per PR
   ============================================================ */
FROM github_demo.repository_management.pull_request_public pr

    /* -- Resolve author name from worker directory -- */
    LEFT JOIN HR.WORKERS worker ON upper(pr.author_id) = upper(worker.account)

    /* -- Resolve manager name by linking worker to their manager -- */
    LEFT JOIN HR.WORKERS manager ON worker.manager_worker_id = manager.worker_id

    /* -- Subquery: pull only the most recent rejection document per PR
          using MAX(submit_date_time) to avoid duplicate rows -- */
    LEFT JOIN (
        SELECT
            wd.pr_id,
            wd.document_type,
            wd.submit_date_time,
            wd.notice,
            wd.submitter
        FROM
            github_demo.repository_management.pr_work_documents wd
        WHERE
            wd.document_type LIKE 'Rejection Notice'
            AND wd.submit_date_time = (
                SELECT MAX(submit_date_time)
                FROM github_demo.repository_management.pr_work_documents wd2
                WHERE wd2.pr_id = wd.pr_id
                AND wd2.document_type LIKE 'Rejection Notice'
            )
    ) rej ON pr.pr_id = rej.pr_id

/* ============================================================
   FILTERS
   Limit to rejected PRs from specific organizations,
   excluding certain support groups, specific author IDs,
   and a specific set of approved reviewers/submitters
   ============================================================ */
WHERE
    pr.status LIKE 'Rejection'                                                   -- Only rejected PRs
    AND pr.author_support_organization IN (                                     -- Specific orgs only
        'GitHub_XYZ',
        'BS_GitHubWorkers',
        'GitHubWorkers',
        'ClientWorkers',
        'CommunityWorkers'
    )
    AND pr.author_support_group NOT IN (                                        -- Exclude these support groups
        'Total_Automation',
        'Clientops_ABC_DEU'
    )
    AND pr.author_id NOT IN (                                                   -- Exclude specific author IDs
        'aa000001',
        'bb000002',
        'cc000003'
    )
    AND rej.submitter IN (                                                      -- Only from approved reviewers
        'aa010101',
        'bb020202',
        'cc030303',
        'dd040404',
        'ee050505',
        'ff060606',
        'gg070707',
        'hh080808',
        'ii090909'
    )
    AND pr.submit_date_time > TO_DATE('2025-01-01', 'YYYY-MM-DD')               -- Submitted after Jan 1 2025

/* ============================================================
   ORDER BY
   Sort results chronologically by scheduled start date
   ============================================================ */
ORDER BY pr.scheduled_start_date_time
