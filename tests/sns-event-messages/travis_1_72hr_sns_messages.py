travis_1_72hr_successful_job_sns_message = {
    "Records": [
        {
            "EventSource": "aws:sns",
            "EventVersion": "1.0",
            "EventSubscriptionArn": "arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results:7b8ea79b-8065-4587-8303-3128664d7615",
            "Sns": {
                "Type": "Notification",
                "MessageId": "ca3a757c-7523-59f7-a8f1-280d359a381e",
                "TopicArn": "arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results",
                "Subject": 'null',
                "Message": "{\"JobId\":\"e5d0cb2133ff575550c96a5d1cf9409c667e06ee2ec8b39bc288d755fbb0f97b\",\"Status\":\"SUCCEEDED\",\"API\":\"StartDocumentAnalysis\",\"Timestamp\":1693924985588,\"DocumentLocation\":{\"S3ObjectName\":\"current/72_HR/Travis_72HR_17AUG23_5e09b83b-d.pdf\",\"S3Bucket\":\"testing-ssa-pdf-store\"}}",
                "Timestamp": "2023-09-05T14:43:05.624Z",
                "SignatureVersion": "1",
                "Signature": "BpgxMch7IvhWWuBMA2GCnJz5Ig2Gt7AL5u40mD+izU5493dHkXCHSIQuP2/qZ1g+mEFHmLceWDUvzx9k+QHZ1qSTfquA4Dc7OohtTGsUpIX3me1dEPY9TRIqp20FBj/aW7F6y7BPbQG+rnlX9WkjLsUKLJixJCuDgFiJnCV20c/MI4QXVf6AemxJPHrcMCXF/P3Wa2QuSJK77iOUOEQvDvEaEGaW7iwmRGERjg/Ta7B2T7zwAH5Y0oO7ihnyuNYjjDvfDDFn9lvki7L0aBCvZ5mhINeprT8JDdNO/JdAumgaEnAa6sfG6ve9nR7GoTxmxB12jkmXFDh5EjyUDQr5RA==",
                "SigningCertUrl": "https://sns.us-east-2.amazonaws.com/SimpleNotificationService-01d088a6f77103d0fe307c0069e40ed6.pem",
                "UnsubscribeUrl": "https://sns.us-east-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results:7b8ea79b-8065-4587-8303-3128664d7615",
                "MessageAttributes": {}
            }
        }
    ]
}
