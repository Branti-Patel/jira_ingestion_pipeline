#Imports + Config
dbutils.widgets.dropdown("RUN_ISSUES", "true", ["true", "false"])
dbutils.widgets.dropdown("RUN_SPRINTS", "true", ["true", "false"])

RUN_ISSUES = dbutils.widgets.get("RUN_ISSUES") == "true"
RUN_SPRINTS = dbutils.widgets.get("RUN_SPRINTS") == "true"


PROJECT_KEYS = ["MYJP", "MYK", "MDP"]

from dotenv import load_dotenv
import os

load_dotenv()

BASE = os.getenv("JIRA_BASE")
EMAIL = os.getenv("JIRA_EMAIL")
API_TOKEN = os.getenv("JIRA_API_TOKEN")

workspace.default.workitems_raw
#Pagination
def fetch_all_issues(PROJECT_KEYS):
    issues_total = []
    next_token = None
    
    while True:
        params = {
            "jql": f"project={project_key}",
            "maxResults": 100,
            "fields": "*all"
        }

        if next_token:
            params["nextPageToken"] = next_token

        resp = requests.get(
            f"{BASE}/rest/api/3/search/jql",
            params=params,
            auth=(EMAIL, API_TOKEN),
            headers={"Accept": "application/json"}
        )

        if resp.status_code != 200:
            raise Exception(f"[{project_key}] Error {resp.status_code}: {resp.text[:300]}")

        data = resp.json()
        page_issues = data.get("issues", [])
        issues_total.extend(page_issues)

        print(f"[{project_key}] page={len(page_issues)} total={len(issues_total)}")

        next_token = data.get("nextPageToken")
        if not next_token:
            break
    
    return issues_total

import urllib.parse
import requests
import json
from pyspark.sql import Row

# Validate credentials are loaded
if not BASE or not EMAIL or not API_TOKEN:
    raise ValueError(
        "Jira credentials not loaded. Ensure BASE, EMAIL, and API_TOKEN are set. "
        "Check that Cell 4 successfully loaded environment variables from .env file."
    )

#  Initialize Rows + Timestamp /ISSUE Extraction

rows = []
from datetime import datetime, UTC
ingestion_ts = datetime.now(UTC).isoformat()# Issue Extraction Loop


for p in PROJECT_KEYS:
    jql = urllib.parse.quote(f"project={p}")
    url = f"{BASE}/rest/api/3/search/jql?jql={jql}&maxResults=100&fields=*all"

    resp = requests.get(url, auth=(EMAIL, API_TOKEN), headers={"Accept": "application/json"})
    print(f"[{p}] Status:", resp.status_code)

    if resp.status_code != 200:
        print(resp.text[:300])
        continue

    issues = resp.json().get("issues", [])

    for issue in issues:
        rows.append(Row(
            project_key=p,
            issue_key=issue.get("key"),
            raw_json=json.dumps(issue),
            ingestion_ts=ingestion_ts
        ))
# Sprint Pagination + Boards Helper (Agile API)

def fetch_all_boards(max_results=50):
    boards = []
    start_at = 0

    while True:
        resp = requests.get(
            f"{BASE}/rest/agile/1.0/board",
            params={"startAt": start_at, "maxResults": max_results},
            auth=(EMAIL, API_TOKEN),
            headers={"Accept": "application/json"}
        )

        data = resp.json()
        page = data.get("values", [])
        boards.extend(page)

        if len(page) < max_results:
            break
        
        start_at += max_results

    return boards

#Sprint Pagination

def fetch_all_sprints_for_board(board_id):
    sprints_total = []
    start_at = 0   

    while True:
        params = {
            "startAt": start_at,
            "maxResults": 50
        }

        resp = requests.get(
            f"{BASE}/rest/agile/1.0/board/{board_id}/sprint",
            params=params,
            auth=(EMAIL, API_TOKEN),
            headers={"Accept": "application/json"}
        )

        data = resp.json()
        page_sprints = data.get("values", [])
        sprints_total.extend(page_sprints)

        if len(page_sprints) < 50:
            break

        start_at += 50
    
    return sprints_total
# Sprint Extraction

sprint_rows = []
sprint_ingestion_ts = datetime.now(UTC).isoformat()

# 1. Get all boards
boards = fetch_all_boards(max_results=50)
print("Boards found:", len(boards))

# 2. For each board, get all sprints
for b in boards:
    board_id = b.get("id")
    if board_id is None:
        continue

    sprints = fetch_all_sprints_for_board(board_id, page_size=50)

    for s in sprints:
        sprint_rows.append(Row(
            board_id=str(board_id),
            sprint_id=str(s.get("id")),
            sprint_name=s.get("name"),
            state=s.get("state"),
            start_date=s.get("startDate"),
            end_date=s.get("endDate"),
            complete_date=s.get("completeDate"),
            raw_json=json.dumps(s),
            ingestion_ts=sprint_ingestion_ts
        ))
#Write to Delta Table

if rows:
    df = spark.createDataFrame(rows)
    df.write.format("delta").mode("overwrite").saveAsTable("workspace.default.workitems_raw")
    print("Saved:", df.count())
else:
    print("No issues found.") 
# Write Sprint Data to Delta (Overwrite Bronze)

if sprint_rows:
    s_df = spark.createDataFrame(sprint_rows)
    s_df.write.format("delta").mode("overwrite").saveAsTable("workspace.default.sprints_raw")
    print("Sprints saved:", s_df.count())
else:
    print("No sprints found.")