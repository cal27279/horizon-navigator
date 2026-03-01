import boto3
import re
import json
import uuid
import os
from datetime import datetime
from botocore.exceptions import NoCredentialsError, ClientError

# --- AWS CONFIGURATION (Hardened) ---
# We use os.getenv to keep your specific bucket/table names private if desired.
# This also prevents the script from failing if these aren't set—it defaults to your project names.
BUCKET_NAME = os.getenv("HORIZON_S3_BUCKET", "horizon-navigator-data")
TABLE_NAME = os.getenv("HORIZON_DYNAMO_TABLE", "HorizonRisks")
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

# --- PMP RISK ENGINE LOGIC ---
RISK_MATRIX = {
    "CRITICAL": ["blocker", "catastrophic", "pii", "resignation", "exploit", "legal", "failure", "outage", "vulnerability"],
    "MODERATE": ["delay", "budget", "latency", "bias", "spike", "inconsistent", "shortage"],
    "MINOR": ["minor", "incomplete", "documentation", "small", "formatting"]
}

# --- GOVERNANCE & SECURITY ---
def sanitize_data(raw_text):
    """Redacts sensitive emails to ensure data privacy before cloud transit."""
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    sanitized_text = re.sub(email_pattern, "[EMAIL_REDACTED]", raw_text)
    return sanitized_text

# --- ENGINE: ARCHITECTING RISK INSIGHTS ---
def classify_severity(text):
    """Categorizes risk level based on PMP-aligned keywords."""
    text = text.lower()
    for level, keywords in RISK_MATRIX.items():
        if any(word in text for word in keywords):
            return level
    return "MINOR"

def process_transcripts(file_path):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔍 Initializing Horizon Navigator Engine...")
    
    if not os.path.exists(file_path):
        print(f"❌ Error: File '{file_path}' not found.")
        return []

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        processed_risks = []
        for line in lines:
            if ":" in line and not line.startswith("---"): 
                sanitized = sanitize_data(line.strip())
                severity = classify_severity(sanitized)
                
                risk_entry = {
                    "RiskID": str(uuid.uuid4())[:8],
                    "Timestamp": datetime.now().isoformat(),
                    "Raw_Insight": sanitized,
                    "Severity": severity,
                    "Project_Phase": "Execution",
                    "Owner": "Carlos Arriaga (PM)"
                }
                processed_risks.append(risk_entry)
                # Reduced print verbosity for cleaner terminal logs
                print(f"[ANALYSIS] {severity:8} | {sanitized[:50]}...")

        return processed_risks

    except Exception as e:
        print(f"❌ Error processing file: {e}")
        return []

# --- CLOUD INTEGRATION ---
def sync_to_aws(data):
    """Handles S3 persistence and DynamoDB indexing using local AWS credentials."""
    try:
        # Boto3 automatically looks for credentials in ~/.aws/credentials
        # or environment variables (AWS_ACCESS_KEY_ID, etc.)
        s3 = boto3.client('s3')
        dynamo = boto3.resource('dynamodb', region_name=REGION)
        table = dynamo.Table(TABLE_NAME)

        # 1. S3 Upload (Audit Trail)
        json_data = json.dumps(data, indent=4)
        file_key = f"logs/risks_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
        s3.put_object(Bucket=BUCKET_NAME, Key=file_key, Body=json_data)
        print(f"✅ [S3] Master log synced to: {file_key}")

        # 2. DynamoDB Indexing (Live Register)
        print(f"⏳ [DynamoDB] Indexing {len(data)} items...")
        with table.batch_writer() as batch:
            for item in data:
                batch.put_item(Item=item)
        print(f"✅ [DynamoDB] Risk Register Table updated successfully.")

    except NoCredentialsError:
        print("❌ Security Error: AWS Credentials not found. Please run 'aws configure'.")
    except ClientError as e:
        print(f"❌ AWS Permission Error: {e.response['Error']['Message']}")
    except Exception as e:
        print(f"❌ Cloud Sync Failed: {e}")

if __name__ == "__main__":
    results = process_transcripts("ai_project_transcripts.txt")
    
    if results:
        sync_to_aws(results)
        print(f"\n🚀 MVP RUN COMPLETE. Data is now live in the AWS Console.")
    else:
        print("⚠️ No data processed. Check your input file.")
