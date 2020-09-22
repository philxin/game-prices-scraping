import boto3
import json
import datetime

client = boto3.client('lambda')

payload = json.dumps({
    "topic": "YOUR-SNS-TOPIC-ARN",
    "subject": "Daily scraping complete.",
    "body": "Today's scraping job is complete. " + str(datetime.datetime.now().strftime("%H:%M:%S"))
})

response = client.invoke(
    FunctionName='trigger_to_sns',
    InvocationType='Event',
    Payload=payload,
)