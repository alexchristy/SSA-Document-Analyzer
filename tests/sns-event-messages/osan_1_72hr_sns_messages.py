osan_1_72hr_successful_job_sns_message = {
    "Records": [
        {
            "EventSource": "aws:sns",
            "EventVersion": "1.0",
            "EventSubscriptionArn": "arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results:7b8ea79b-8065-4587-8303-3128664d7615",
            "Sns": {
                "Type": "Notification",
                "MessageId": "a12bd683-e5e6-53c4-b57c-8bec0d3cd510",
                "TopicArn": "arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results",
                "Subject": 'null',
                "Message": "{\"JobId\":\"5359f6d7c2c4ae1bc6f9024fbaf5e04492970f27651f2c63272758d23cdcba8b\",\"Status\":\"SUCCEEDED\",\"API\":\"StartDocumentAnalysis\",\"Timestamp\":1693836954153,\"DocumentLocation\":{\"S3ObjectName\":\"current/72_HR/OSAN_72HR_18AUG2023_fe65a56a-1.pdf\",\"S3Bucket\":\"testing-ssa-pdf-store\"}}",
                "Timestamp": "2023-09-04T14:15:54.196Z",
                "SignatureVersion": "1",
                "Signature": "D5gbGh38oTE43hZ1p9URILPhAkVOE/LxpDgiVtyTPXgIqT+0tA98dtCNnPmKK/M2eN595B76s93ppR7VvmrEfeZlbmbeKTXrTjZUNKAI+v+JFyTjIYApYrrJoZLXVJbBtajtb+MFvPo1YihGdz7enmQXTmczkHLajlZXP+cnOXy5Mk2URMq3XSnc4ZZkN9ctu5keUseud11Okslvt5ClNFOp6TXOPy+I26YDk+I/ElSsVmvBTmfIS0TgcLf02ZZ2zviBL0MQjFCdNva2ZbWetPglhhr9wOm7en9mvqoATVufjKA89R++HdesR7wRDDGssgeRLSsFW2PXNTT/r/bmvg==",
                "SigningCertUrl": "https://sns.us-east-2.amazonaws.com/SimpleNotificationService-01d088a6f77103d0fe307c0069e40ed6.pem",
                "UnsubscribeUrl": "https://sns.us-east-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results:7b8ea79b-8065-4587-8303-3128664d7615",
                "MessageAttributes": {}
            }
        }
    ]
}
