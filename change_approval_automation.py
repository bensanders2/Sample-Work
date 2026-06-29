"""
change_approval_automation.py
------------------------------
Portfolio demo: End-to-end change management approval automation.

Workflow:
  1. Query Snowflake for completed PRs/change tickets that are awaiting approval
  2. Evaluate each ticket against compliance rules (required docs, task reviews)
  3. POST an approval or rejection work log entry via the Helix REST API
  4. Mark approved tickets via PATCH; flag deficient tickets with missing field notes

Demonstrates:
  - Snowflake connector with .env-based credential management
  - Multi-join CTE-based compliance audit query (PRESENT / MISSING pattern)
  - OAuth2 token acquisition and auto-refresh for Helix ITSM
  - Conditional approval logic driven by SQL output
  - Structured work log posting via REST API
  - Retry logic for transient API failures

Author: Ben Sanders
Context: Federal IT / DoD-VA Change Management automation
"""

import os
import time
import logging
import requests
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# --- Snowflake credentials (from .env) ---
SF_ACCOUNT   = os.getenv("SF_ACCOUNT")
SF_USER      = os.getenv("SF_USER")
SF_PASSWORD  = os.getenv("SF_PASSWORD")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE")
SF_DATABASE  = os.getenv("SF_DATABASE")
SF_SCHEMA    = os.getenv("SF_SCHEMA")

# --- Helix API config ---
BASE_URL     = os.getenv("HELIX_BASE_URL", "https://your-helix-instance.example.com")
TOKEN_URL    = f"{BASE_URL}/api/jwt/login"
TICKET_API   = f"{BASE_URL}/api/arsys/v1/entry/CHG:ChangeInterface"
WORKLOG_API  = f"{BASE_URL}/api/arsys/v1/entry/CHG:WorkLog"

CLIENT_ID     = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

MAX_RETRIES  = 3
RETRY_DELAY  = 5

# Compliance columns that must all be 'PRESENT' for auto-approval
REQUIRED_FIELDS = [
    "Work Instruction",
    "Backout Steps",
    "Proof of Completion",
    "Team Review",
    "Senior Team Review",
    "Notes",
    "Change Review",
    "Repository",
    "Location",
]

# Fields that are impact-conditional (NOT REQUIRED is acceptable for Medium)
CONDITIONAL_FIELDS = ["Client Approval", "Co-Driver"]


# ---------------------------------------------------------------------------
# Snowflake: Pull compliance data
# ---------------------------------------------------------------------------

COMPLIANCE_QUERY = """
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
        pr.status IN ('Finished')
        AND pr.submit_date_time > '2024-01-01'
        AND pr.is_fork = 0
        AND pr.impact IN ('Medium', 'High')
        AND pr.author_support_organization IN ('GitHub_XYZ', 'BS_GitHubWorkers', 'GitHubWorkers')
        AND pr.author_support_group <> 'Clientops_ABC_'
        AND pr.change_class = 'Normal'
        AND pr.awaiting_approval = 'Yes'
)

SELECT
    pr.pr_id                                                        AS "PR ID",
    pr.impact                                                       AS "Impact",
    pr.status                                                       AS "Status",
    pr.awaiting_approval                                            AS "Awaiting Approval",

    TO_VARCHAR(pr.scheduled_start_date_time, 'MM/DD/YYYY HH12:MI:SS AM') AS "Scheduled Start Date",
    TO_VARCHAR(pr.scheduled_end_date_time,   'MM/DD/YYYY HH12:MI:SS AM') AS "Scheduled End Date",

    CASE WHEN pr.author_id NOT IN ('NS') THEN pr.author_id
         ELSE pr.review_manager_id END                              AS "Author ID",

    CASE WHEN pr.author_id NOT IN ('NS') THEN manager.account
    END                                                             AS "Manager ID",

    CASE WHEN LENGTH(pr.description) > 10 THEN 'PRESENT'
         ELSE 'MISSING' END                                         AS "Notes",

    pr.author_support_group                                         AS "Support Group",
    pr.author_support_organization                                  AS "Support Org",

    CASE WHEN pr.reason_for_review <> 'NS' THEN 'PRESENT'
         ELSE 'MISSING' END                                         AS "Change Review",

    CASE WHEN pr.repository_name <> 'NS' THEN 'PRESENT'
         ELSE 'MISSING' END                                         AS "Repository",

    -- Work document checks (MAX collapses multiple work info rows per ticket)
    MAX(CASE WHEN (info.work_info_type = 'Work Instruction' AND LENGTH(info.notes) > 20)
              OR  (info.work_info_type = 'Work Instruction'
                   AND (info.attachment_1 <> 'NS' OR info.attachment_2 <> 'NS' OR info.attachment_3 <> 'NS'))
             THEN 'PRESENT' ELSE 'MISSING' END)                     AS "Work Instruction",

    MAX(CASE WHEN (info.work_info_type = 'Backout Steps' AND LENGTH(info.notes) > 20)
              OR  (info.work_info_type = 'Backout Steps'
                   AND (info.attachment_1 <> 'NS' OR info.attachment_2 <> 'NS' OR info.attachment_3 <> 'NS'))
             THEN 'PRESENT' ELSE 'MISSING' END)                     AS "Backout Steps",

    MAX(CASE WHEN (info.work_info_type = 'Client Approval'
                   AND info.notes LIKE 'Approved by Client Reviewer%') THEN 'PRESENT'
             WHEN pr.impact = 'Medium' THEN 'NOT REQUIRED'
             ELSE 'MISSING' END)                                    AS "Client Approval",

    MAX(CASE WHEN (info.work_info_type = 'Proof of Completion'
                   AND info.notes IS NOT NULL AND LENGTH(info.notes) > 0
                   AND info.notes <> 'Example Default Language When Creating Proof of Completion')
              OR  (info.work_info_type = 'Proof of Completion'
                   AND (info.attachment_1 <> 'NS' OR info.attachment_2 <> 'NS' OR info.attachment_3 <> 'NS'))
             THEN 'PRESENT' ELSE 'MISSING' END)                     AS "Proof of Completion",

    -- Task review checks
    MAX(CASE WHEN tsk.task_name LIKE '%Team Review'
              AND tsk.status = 'Closed' AND tsk.status_reason = 'Success'
              AND tsk.assignee_id <> pr.author_id
             THEN 'PRESENT' ELSE 'MISSING' END)                     AS "Team Review",

    MAX(CASE WHEN tsk.task_name LIKE 'Senior Team Review'
              AND tsk.status = 'Closed' AND tsk.status_reason = 'Success'
              AND tsk.assignee_id <> pr.author_id
             THEN 'PRESENT' ELSE 'MISSING' END)                     AS "Senior Team Review",

    MAX(CASE WHEN (tsk.task_name LIKE '%co-driver'
                   AND tsk.status = 'Closed' AND tsk.assignee_id <> pr.author_id) THEN 'PRESENT'
             WHEN pr.impact = 'Medium' THEN 'NOT REQUIRED'
             ELSE 'MISSING' END)                                    AS "Co-Driver",

    CASE WHEN pr.environment <> 'NS' THEN 'PRESENT'
         ELSE 'MISSING' END                                         AS "Location"

FROM filtered_changes AS pr
    LEFT JOIN HR.WORKERS worker  ON upper(pr.author_id)         = upper(worker.account)
    LEFT JOIN HR.WORKERS manager ON worker.manager_worker_id    = manager.worker_id
    LEFT JOIN github_demo.repository_management.pr_related_tickets AS rln ON pr.pr_id = rln.pr_id
    LEFT JOIN github_demo.repository_management.pr_work_documents  AS info ON pr.pr_id = info.pr_id
    LEFT JOIN github_demo.repository_management.pr_work_task       AS tsk  ON pr.pr_id = tsk.parent_id

WHERE
    rln.request_type = 'Software Component'
    AND rln.relationship_type = 'Direct'

GROUP BY
    pr.pr_id, pr.description, pr.reason_for_review, pr.repository_name,
    pr.scheduled_start_date_time, pr.environment, rln.relationship_type,
    rln.request_type, pr.impact, pr.scheduled_end_date_time,
    pr.submit_date_time, pr.author, pr.author_support_group,
    pr.author_support_organization, pr.review_manager, pr.status,
    pr.template_name, pr.is_fork, pr.author_id, pr.review_manager_id,
    manager.account
"""


def query_snowflake() -> pd.DataFrame:
    """Connect to Snowflake and return the compliance audit results as a DataFrame."""
    log.info("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        password=SF_PASSWORD,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
    )
    try:
        cur = conn.cursor()
        cur.execute(COMPLIANCE_QUERY)
        df = cur.fetch_pandas_all()
        log.info("Retrieved %d tickets awaiting approval.", len(df))
        return df
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Compliance evaluation
# ---------------------------------------------------------------------------

def get_rejection_note(missing_cols: list[str]) -> str:
    """Build a human-readable rejection message listing missing fields."""
    if not missing_cols:
        return "Approve"
    items = ", ".join(missing_cols)
    # mirror the real script's grammar cleanup
    note = f"Please add {items}"
    note = note.replace("add ensure", "ensure").replace("add assign", "assign")
    return note


def evaluate_compliance(row: pd.Series) -> tuple[bool, str]:
    """
    Determine whether a ticket passes compliance.

    Returns:
        (approved: bool, message: str)
    """
    missing = [col for col in REQUIRED_FIELDS if row.get(col) == "MISSING"]

    # Conditional fields: only fail if 'MISSING' (NOT REQUIRED is acceptable)
    missing += [col for col in CONDITIONAL_FIELDS if row.get(col) == "MISSING"]

    if missing:
        return False, get_rejection_note(missing)
    return True, "Approve"


# ---------------------------------------------------------------------------
# Helix API: Token management
# ---------------------------------------------------------------------------

class TokenManager:
    """Handles OAuth2 token acquisition and automatic refresh."""

    def __init__(self):
        self._token: str | None = None
        self._expires_at: float = 0.0

    def get_token(self) -> str:
        if self._token and time.time() < self._expires_at - 60:
            return self._token
        return self._refresh()

    def _refresh(self) -> str:
        log.info("Acquiring OAuth2 token...")
        r = requests.post(
            TOKEN_URL,
            data={"grant_type": "client_credentials",
                  "client_id": CLIENT_ID,
                  "client_secret": CLIENT_SECRET},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        self._token = data["access_token"]
        self._expires_at = time.time() + data.get("expires_in", 3600)
        log.info("Token acquired.")
        return self._token

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self.get_token()}",
                "Content-Type": "application/json"}


# ---------------------------------------------------------------------------
# Helix API: Work log and approval actions
# ---------------------------------------------------------------------------

def _api_call(method: str, url: str, tokens: TokenManager, **kwargs) -> requests.Response:
    """Shared request wrapper with retry and 401 token-refresh logic."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.request(method, url, headers=tokens._headers(),
                                    timeout=15, **kwargs)
            if resp.status_code == 401:
                log.warning("401 — refreshing token and retrying.")
                tokens._refresh()
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            log.error("Attempt %d/%d failed: %s", attempt, MAX_RETRIES, exc)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    raise RuntimeError(f"API call to {url} failed after {MAX_RETRIES} attempts.")


def post_worklog(change_id: str, notes: str, worklog_type: str,
                 tokens: TokenManager) -> None:
    """Add a work log entry to a change ticket."""
    payload = {
        "values": {
            "Change ID": change_id,
            "Work Log Type": worklog_type,
            "Detailed Description": notes,
        }
    }
    _api_call("POST", WORKLOG_API, tokens, json=payload)
    log.info("Work log posted to %s [%s].", change_id, worklog_type)


def approve_ticket(change_id: str, tokens: TokenManager) -> None:
    """PATCH the ticket status to Approved."""
    payload = {"values": {"Status": "Approved"}}
    _api_call("PATCH", f"{TICKET_API}/{change_id}", tokens, json=payload)
    log.info("Ticket %s approved.", change_id)


def flag_for_review(change_id: str, notes: str, tokens: TokenManager) -> None:
    """Post a rejection work log without changing ticket status (manual follow-up required)."""
    post_worklog(change_id, notes, worklog_type="Change Assessment", tokens=tokens)
    log.info("Ticket %s flagged — missing: %s", change_id, notes)


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def run_approvals() -> None:
    df = query_snowflake()
    df.index = df["PR ID"].tolist()

    tokens = TokenManager()
    approved, flagged = [], []

    for change_id, row in df.iterrows():
        is_compliant, message = evaluate_compliance(row)

        if is_compliant:
            post_worklog(
                change_id=change_id,
                notes=f"This change was automatically granted post-implementation approval by {os.getenv('CURRENT_USER', 'automation')}.",
                worklog_type="Change Assessment",
                tokens=tokens,
            )
            approve_ticket(change_id, tokens)
            df.at[change_id, "Change Assessment Notes"] = "Approve"
            approved.append(change_id)
        else:
            flag_for_review(change_id, message, tokens)
            df.at[change_id, "Change Assessment Notes"] = message
            flagged.append(change_id)

    log.info("Run complete — Approved: %d | Flagged: %d", len(approved), len(flagged))


if __name__ == "__main__":
    run_approvals()
