#!/usr/bin/env python3
"""
Check for duplicate code_hash values in BigQuery dataset
"""

from google.cloud import bigquery
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/ubuntu/git/sourcify/services/sync-bigquery/gcp-key.json'

client = bigquery.Client(project='n8n-self-468309')

print("=" * 80)
print("Checking for duplicate code_hash values in public_code")
print("=" * 80)

# Check for duplicates
query = """
SELECT 
  COUNT(*) as total_rows,
  COUNT(DISTINCT code_hash) as unique_code_hashes,
  COUNT(*) - COUNT(DISTINCT code_hash) as duplicate_count
FROM `n8n-self-468309.sourcify_dataset.public_code`
"""

result = client.query(query).result()
for row in result:
    print(f"\nTotal rows: {row.total_rows:,}")
    print(f"Unique code_hash values: {row.unique_code_hashes:,}")
    print(f"Duplicate rows: {row.duplicate_count:,}")
    print(f"Duplicate percentage: {(row.duplicate_count / row.total_rows * 100):.2f}%")

# Show some examples of duplicates
print("\n" + "=" * 80)
print("Sample duplicate code_hash values:")
print("=" * 80)

query2 = """
SELECT code_hash, COUNT(*) as count
FROM `n8n-self-468309.sourcify_dataset.public_code`
GROUP BY code_hash
HAVING COUNT(*) > 1
ORDER BY count DESC
LIMIT 10
"""

result2 = client.query(query2).result()
for i, row in enumerate(result2, 1):
    print(f"{i}. code_hash appears {row.count} times")

print("\n" + "=" * 80)
