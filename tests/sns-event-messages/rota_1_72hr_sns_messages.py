rota_1_72hr_successful_job_sns_message = {
    "Records": [
        {
            "EventSource": "aws:sns",
            "EventVersion": "1.0",
            "EventSubscriptionArn": "arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results:7b8ea79b-8065-4587-8303-3128664d7615",
            "Sns": {
                "Type": "Notification",
                "MessageId": "b961bf8d-517f-5fc5-be1a-e2e2a1c6fb4b",
                "TopicArn": "arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results",
                "Subject": 'null',
                "Message": "{\"JobId\":\"e87ab69183eccde26160da6de8e3dc0e5943c6f0212348119dedc2f1af16056e\",\"Status\":\"SUCCEEDED\",\"API\":\"StartDocumentAnalysis\",\"Timestamp\":1693838969469,\"DocumentLocation\":{\"S3ObjectName\":\"current/72_HR/ROTA_72HR_16AUG2023_03d9ee92-c.pdf\",\"S3Bucket\":\"testing-ssa-pdf-store\"}}",
                "Timestamp": "2023-09-04T14:49:29.522Z",
                "SignatureVersion": "1",
                "Signature": "kmh+qLAjkGsD0VwUFB/mXAhQL+YN1CtPDx6OXGzAZMiV1kVlTFVQqeTniu7/ikGZ9/ebMUHZdzZ4ejv/HhZ8jBHTmazm1KIVadm7z8AgSQv8GBgzOBTSKcutqd2cM54NDTGvh1dYo5aN+h7v4LV7rqARwJ92mfmk5YgQTa4jYTL32CLB3+7uT7CoZLyW9YX5zoB6CvjGYVCHgaN6hqhxZE9rdC5O4ve/RQPb/qT6J/ZeSt+3SKStyQ3pwHrPaj9Cm0NLVHuoAeYTR//rsvUbwznWzOiMwUTW+bXM9xYPjUOlzatxVQhPbB1fFQ983cxVKKXhR3646bUKWyMePi7zTA==",
                "SigningCertUrl": "https://sns.us-east-2.amazonaws.com/SimpleNotificationService-01d088a6f77103d0fe307c0069e40ed6.pem",
                "UnsubscribeUrl": "https://sns.us-east-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results:7b8ea79b-8065-4587-8303-3128664d7615",
                "MessageAttributes": {}
            }
        }
    ]
}
