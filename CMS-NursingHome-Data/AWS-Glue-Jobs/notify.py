"""
Glue Job 4 — Workflow notification publisher (SNS).

Sends a success/failure email via SNS at the end of a Glue Workflow run.
Glue Workflow has two trigger paths that both invoke this job with
different --status / --message arguments.

Job type: Python Shell (not PySpark). Faster startup, cheaper, right-sized
for a single API call.

Args (Glue Job Parameters):
  --topic_arn             SNS topic ARN
  --status                'success' or 'failure'
  --workflow_name         Name of the parent workflow (for the email subject)
  --message               Free-form context (job names, error pointer, etc.)
"""

import sys
from datetime import datetime, timezone

import boto3
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(
    sys.argv,
    ["topic_arn", "status", "workflow_name", "message"],
)

now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
status = args["status"].lower()
icon = "[OK]" if status == "success" else "[FAIL]"

# SNS subjects must be ASCII and <= 100 chars
subject = f"{icon} {args['workflow_name']} - {status}"[:99]

body = f"""\
Workflow:  {args['workflow_name']}
Status:    {status}
Time:      {now}

Details:
{args['message']}

If status is failure, check CloudWatch logs under
/aws-glue/jobs/error for the failed run.
"""

sns = boto3.client("sns")
resp = sns.publish(
    TopicArn=args["topic_arn"],
    Subject=subject,
    Message=body,
)
print(f"[notify] published to {args['topic_arn']}, MessageId={resp['MessageId']}")
print(f"[notify] subject: {subject}")
