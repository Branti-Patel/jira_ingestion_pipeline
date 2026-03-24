import requests
from requests.auth import HTTPBasicAuth
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable

# Load environment variables
load_dotenv()

BASE = os.getenv("JIRA_BASE")
EMAIL = os.getenv("JIRA_EMAIL")
API_TOKEN = os.getenv("JIRA_API_TOKEN")


PROJECT_KEY = "MYJP"

JQL = f"project = {PROJECT_KEY} ORDER BY updated DESC"
FIELDS = "key,summary,issuetype,status,created,updated,duedate,assignee"
MAX_RESULTS = 200

auth = HTTPBasicAuth(EMAIL, API_TOKEN)
headers = {"Accept": "application/json"}
params = {"jql": JQL, "fields": FIELDS, "maxResults": str(MAX_RESULTS)}

resp = requests.get(
    f"{BASE}/rest/api/3/search/jql",
    headers=headers,
    auth=auth,
    params=params,
    timeout=60
)
resp.raise_for_status()

data = resp.json()
issues = data.get("issues", [])


rows = []
for it in issues:
    f = it.get("fields", {}) or {}
    rows.append({
        "issue_key": it.get("key"),
        "summary": f.get("summary"),
        "issuetype": (f.get("issuetype") or {}).get("name"),
        "status": (f.get("status") or {}).get("name"),
        "created": f.get("created"),
        "updated": f.get("updated"),
        "duedate": f.get("duedate"),
        "assignee": (f.get("assignee") or {}).get("displayName")
    })


schema = StructType([
    StructField("issue_key", StringType(), True),
    StructField("summary", StringType(), True),
    StructField("issuetype", StringType(), True),
    StructField("status", StringType(), True),
    StructField("created", StringType(), True),
    StructField("updated", StringType(), True),
    StructField("duedate", StringType(), True),
    StructField("assignee", StringType(), True)
])


df = spark.createDataFrame(rows, schema=schema) \
    .withColumn("created_ts", F.to_timestamp("created")) \
    .withColumn("updated_ts", F.to_timestamp("updated")) \
    .withColumn("due_dt", F.to_date("duedate")) \
    .withColumn("load_date", F.current_date())

display(df)
df.createOrReplaceTempView("df"
                           )

spark.sql("""
CREATE TABLE IF NOT EXISTS jira_job1 (
  issue_key  STRING,
  summary    STRING,
  issuetype  STRING,
  status     STRING,
  created    STRING,
  updated    STRING,
  duedate    STRING,
  assignee   STRING,
  created_ts TIMESTAMP,
  updated_ts TIMESTAMP,
  due_dt     DATE,
  load_date  DATE
) USING DELTA
""")



target = DeltaTable.forName(spark, "jira_job1")

(
    target.alias("t")
    .merge(
        df.alias("s"),
        "t.issue_key = s.issue_key"
    )
    .whenMatchedUpdate(set={
        "summary": "s.summary",
        "issuetype": "s.issuetype",
        "status": "s.status",
        "created": "s.created",
        "updated": "s.updated",
        "duedate": "s.duedate",
        "assignee": "s.assignee",
        "created_ts": "s.created_ts",
        "updated_ts": "s.updated_ts",
        "due_dt": "s.due_dt",
        "load_date": "s.load_date"
    })
    .whenNotMatchedInsertAll()
    .execute()
)

print("UPSERT completed: jira_job1")
