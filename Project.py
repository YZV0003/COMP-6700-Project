import pandas as pd

# -------------------------
# Load data from Hugging Face
# -------------------------

# Task 1 table
all_pr_df = pd.read_parquet("hf://datasets/hao-li/AIDev/all_pull_request.parquet")

# Task 2 table
all_repo_df = pd.read_parquet("hf://datasets/hao-li/AIDev/all_repository.parquet")

# Task 3 table
pr_task_df = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_task_type.parquet")

# Task 4 table
pr_commit_df = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_commit_details.parquet")

# -------------------------
#Task 1: all_pull_request → task1_all_pull_request.csv
# -------------------------

task1_df = all_pr_df[
    ['title', 'id', 'agent', 'body', 'repo_id', 'repo_url']
].rename(columns={
    'title':   'TITLE',
    'id':      'ID',
    'agent':   'AGENTNAME',
    'body':    'BODYSTRING',
    'repo_id': 'REPOID',
    'repo_url': 'REPOURL',
})

task1_df.to_csv("task1_all_pull_request.csv", index=False)
print("wrote task1_all_pull_request.csv")

# -------------------------
# Task 2: all_repository → task2_all_repository.csv
# -------------------------

task2_df = all_repo_df[
    ['id', 'language', 'stars', 'url']
].rename(columns={
    'id':       'REPOID',
    'language': 'LANG',
    'stars':    'STARS',
    'url':      'REPOURL',
})

task2_df.to_csv("task2_all_repository.csv", index=False)
print("wrote task2_all_repository.csv")

# -------------------------
# Task 3: pr_task_type → task3_pr_task_type.csv
# -------------------------
# Columns per data_table.md: agent, id, title, reason, type
# Some versions may also have a "confidence" column; if not, fill with NA.

if 'confidence' in pr_task_df.columns:
    confidence_series = pr_task_df['confidence']
else:
    confidence_series = pd.NA

task3_df = pd.DataFrame({
    'PRID':        pr_task_df['id'],
    'PRTITLE':     pr_task_df['title'],
    'PRREASON':    pr_task_df['reason'],
    'PRTYPE':      pr_task_df['type'],
    'CONFIDENCE':  confidence_series,
})

task3_df.to_csv("task3_pr_task_type.csv", index=False)
print("wrote task3_pr_task_type.csv")

# -------------------------
# Task 4: pr_commit_details → task4_pr_commit_details.csv
# -------------------------
# Columns per data_table.md:
# sha, pr_id, message, filename, status, additions, deletions, changes, patch, ...

def clean_patch(text):
    """Remove newlines / tabs etc. from patch so it is CSV-friendly."""
    if pd.isna(text):
        return text
    s = str(text)
    s = s.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    # optional: also remove commas to be extra safe in CSV
    s = s.replace(",", " ")
    return s

pr_commit_df['clean_patch'] = pr_commit_df['patch'].apply(clean_patch)

task4_df = pd.DataFrame({
    'PRID':          pr_commit_df['pr_id'],
    'PRSHA':         pr_commit_df['sha'],
    'PRCOMMITMESSAGE': pr_commit_df['message'],
    'PRFILE':        pr_commit_df['filename'],
    'PRSTATUS':      pr_commit_df['status'],
    'PRADDS':        pr_commit_df['additions'],
    'PRDELSS':       pr_commit_df['deletions'],
    'PRCHANGECOUNT': pr_commit_df['changes'],
    'PRDIFF':        pr_commit_df['clean_patch'],
})

task4_df.to_csv("task4_pr_commit_details.csv", index=False)
print("wrote task4_pr_commit_details.csv")

# -------------------------
# 6. Task 5: Security label per pull request
# -------------------------
# Uses:
#   task1_df: Task 1 (TITLE, ID, AGENTNAME, BODYSTRING, ...)
#   task3_df: Task 3 (PRID, PRTYPE, CONFIDENCE, ...)

# 1) Merge Task 1 and Task 3 on PR id
t5 = task1_df.merge(
    task3_df[["PRID", "PRTYPE", "CONFIDENCE"]],
    left_on="ID",
    right_on="PRID",
    how="left"
)

# 2) Keep only rows that actually have a TYPE (PRTYPE not null)
t5 = t5.dropna(subset=["PRTYPE"])

# 3) EXACT security-related keywords from the assignment
security_keywords = [
    "race", "racy", "buffer", "overflow", "stack", "integer",
    "signedness", "underflow", "improper", "unauthenticated",
    "gain access", "permission", "cross site", "css", "xss",
    "denial service", "dos", "crash", "deadlock", "injection",
    "request forgery", "csrf", "xsrf", "forged", "security",
    "vulnerability", "vulnerable", "exploit", "attack", "bypass",
    "backdoor", "threat", "expose", "breach", "violate", "fatal",
    "blacklist", "overrun", "insecure"
]

def compute_security(row):
    # Only title + body, per instructions
    text = f"{row.get('TITLE', '')} {row.get('BODYSTRING', '')}".lower()
    return int(any(kw in text for kw in security_keywords))

t5["SECURITY"] = t5.apply(compute_security, axis=1)

# 4) Final Task-5 dataframe with required columns
task5_df = t5.rename(columns={
    "AGENTNAME": "AGENT",
    "PRTYPE": "TYPE"
})[["ID", "AGENT", "TYPE", "CONFIDENCE", "SECURITY"]]

task5_df.to_csv("task5_security_label.csv", index=False)
print("wrote task5_security_label.csv")
