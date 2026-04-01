import duckdb
import pandas as pd
import re

conn = duckdb.connect()

PARQUET_PATH = r'C:\Users\conor\Downloads\emails-slim.parquet'

# ── 1. Row count ────────────────────────────────────────────────────────────
print("=== ROW COUNT ===")
count = conn.sql(f"""
    SELECT COUNT(*) FROM read_parquet('{PARQUET_PATH}')
""").df()
print(count)

# ── 2. Schema ───────────────────────────────────────────────────────────────
print("\n=== SCHEMA ===")
df = conn.sql(f"""
    SELECT * FROM read_parquet('{PARQUET_PATH}')
    LIMIT 5
""").df()
print(df.columns.tolist())
print(df.dtypes)

# ── 3. Promotional vs non-promotional breakdown ─────────────────────────────
print("\n=== PROMOTIONAL BREAKDOWN ===")
summary = conn.sql(f"""
    SELECT
        is_promotional,
        epstein_is_sender,
        COUNT(*) as n
    FROM read_parquet('{PARQUET_PATH}')
    GROUP BY is_promotional, epstein_is_sender
""").df()
print(summary)

# ── 4. Top 100 senders (non-promotional) ────────────────────────────────────
print("\n=== TOP 100 SENDERS ===")
top_senders = conn.sql(f"""
    SELECT
        sender,
        COUNT(*) as n
    FROM read_parquet('{PARQUET_PATH}')
    WHERE is_promotional = false
    AND sender IS NOT NULL
    GROUP BY sender
    ORDER BY n DESC
    LIMIT 100
""").df()
print(top_senders.to_string())

# ── 5. Top recipient email addresses ────────────────────────────────────────
print("\n=== TOP RECIPIENT EMAILS ===")
df_sample = conn.sql(f"""
    SELECT to_recipients, cc_recipients
    FROM read_parquet('{PARQUET_PATH}')
    WHERE is_promotional = false
    LIMIT 50000
""").df()

emails = []
for col in ['to_recipients', 'cc_recipients']:
    for val in df_sample[col].dropna():
        found = re.findall(r'[\w\.\-]+@[\w\.\-]+', str(val))
        emails.extend(found)

email_series = pd.Series(emails)
print(email_series.value_counts().head(50))

# ── 6. Sample raw JSON structure ─────────────────────────────────────────────
print("\n=== SAMPLE RAW ROWS ===")
df_raw = conn.sql(f"""
    SELECT sender, to_recipients, cc_recipients
    FROM read_parquet('{PARQUET_PATH}')
    WHERE is_promotional = false
    LIMIT 20
""").df()
for i, row in df_raw.iterrows():
    print(f"SENDER: {row['sender']}")
    print(f"TO:     {row['to_recipients']}")
    print(f"CC:     {row['cc_recipients']}")
    print("---")