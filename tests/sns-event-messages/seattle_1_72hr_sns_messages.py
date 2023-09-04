seattle_1_72hr_successful_job_sns_message = {
    "Records": [
        {
            "EventSource": "aws:sns",
            "EventVersion": "1.0",
            "EventSubscriptionArn": "arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results:7b8ea79b-8065-4587-8303-3128664d7615",
            "Sns": {
                "Type": "Notification",
                "MessageId": "95c517b8-df34-5016-826a-4ad72c5ad478",
                "TopicArn": "arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results",
                "Subject": 'null',
                "Message": "{\"JobId\":\"c70c1932353cecadc088722430324cb1d5ea83483691feeca4bbc87af63c6ac7\",\"Status\":\"SUCCEEDED\",\"API\":\"StartDocumentAnalysis\",\"Timestamp\":1693839662113,\"DocumentLocation\":{\"S3ObjectName\":\"current/72_HR/SEA72hr_26a441b1-1.pdf\",\"S3Bucket\":\"testing-ssa-pdf-store\"}}",
                "Timestamp": "2023-09-04T15:01:02.173Z",
                "SignatureVersion": "1",
                "Signature": "f6i/biOFmyYI9Zf9XnuWrksBYVBJd+AK4R1NragkxsszEHYod8yvExIN2Sv6THw6nw84GJEM6bVGlWiYWjqI5JNGKwN5OxvGsPDWpwA/H0MRKqtT3zwQLqM32R+X04a11S9hHRsY/Kv23T4McDwoL/OmCzxT3B2LvEtzjMKxzOravV+gizyognOOnsdtwjzQYISwnXZduVZq1PrKPuwPTa4B8VU4cBJnLZ9++w81P7Y8LIXYpjZOB7PngDiNShuxLDvWP0uSS7jj5iFwBXLHBX3kwSCQvy+eAff6Pg9ZgrTMkXGdBaE+pngWr3a7EOuMd6RIgrnuAultLWkZC4Vh5A==",
                "SigningCertUrl": "https://sns.us-east-2.amazonaws.com/SimpleNotificationService-01d088a6f77103d0fe307c0069e40ed6.pem",
                "UnsubscribeUrl": "https://sns.us-east-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results:7b8ea79b-8065-4587-8303-3128664d7615",
                "MessageAttributes": {}
            }
        }
    ]
}
