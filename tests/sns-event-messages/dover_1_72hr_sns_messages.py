dover_1_72hr_successful_job_sns_message = {
    "Records": [
        {
            "EventSource": "aws:sns",
            "EventVersion": "1.0",
            "EventSubscriptionArn": "arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results:7b8ea79b-8065-4587-8303-3128664d7615",
            "Sns": {
                "Type": "Notification",
                "MessageId": "5a101022-1306-5df4-9fe7-4d4c980bc1ba",
                "TopicArn": "arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results",
                "Subject": 'null',
                "Message": "{\"JobId\":\"e843f7cf5eca4892b3115c4bb46e04934bece9ce947949439ed59fa66a79e34a\",\"Status\":\"SUCCEEDED\",\"API\":\"StartDocumentAnalysis\",\"Timestamp\":1693747405661,\"DocumentLocation\":{\"S3ObjectName\":\"current/72_HR/DOVER_72HR_18AUG23_2cd97450-f.pdf\",\"S3Bucket\":\"testing-ssa-pdf-store\"}}",
                "Timestamp": "2023-09-03T13:23:25.726Z",
                "SignatureVersion": "1",
                "Signature": "c6GB6AySOC9IS1YU0QmEY4DKyp2AHe+lRIXLtlXYu6TWBazwntD99FFDSkDjI69hER1Id4vsTmCiWPRLiWp/cuvwXwd6oajGGlxJDhl9/QokUh/2ckid/hLu3W0mAkIaR+0PqCmHOf8lWdp/LrkHvRJb4XFgrxvRrox8oc785HkqWsYIGrkUu3oWQMPiVvFnkazI0xx94xMppYtIt2C4EJLom83MKWW6b35vUhemXLcVTv35UbbhQnHlnrWq8ehlaCJDWA69ICQOFn6UQ3Xso0dreTbKsoN+GEVES0e+s6CsqAdca/6fyz3oYr0dfhkbiYYnyjcimFkspO1Q9da8Bw==",
                "SigningCertUrl": "https://sns.us-east-2.amazonaws.com/SimpleNotificationService-01d088a6f77103d0fe307c0069e40ed6.pem",
                "UnsubscribeUrl": "https://sns.us-east-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results:7b8ea79b-8065-4587-8303-3128664d7615",
                "MessageAttributes": {}
            }
        }
    ]
}
