mcconnell_2_72hr_successful_job_sns_message = {
    "Records": [
        {
            "EventSource": "aws:sns",
            "EventVersion": "1.0",
            "EventSubscriptionArn": "arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results:7b8ea79b-8065-4587-8303-3128664d7615",
            "Sns": {
                "Type": "Notification",
                "MessageId": "38e016b4-45e7-5477-af5b-4b155f3d24e4",
                "TopicArn": "arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results",
                "Subject": 'null',
                "Message": "{\"JobId\":\"c1daa6124fe4d2a517cf6e3896e7481ffd1f6fa16ed91042fcf2c6ba7a4f8804\",\"Status\":\"SUCCEEDED\",\"API\":\"StartDocumentAnalysis\",\"Timestamp\":1697061018003,\"DocumentLocation\":{\"S3ObjectName\":\"archive/McConnell_AFB_Air_Transportation_Function/72_HR/mcconnell_2_72hr_test.pdf\",\"S3Bucket\":\"testing-ssa-pdf-store\"}}",
                "Timestamp": "2023-10-11T21:50:18.062Z",
                "SignatureVersion": "1",
                "Signature": "MkHKkwwO+ycibyq5ak6TqQt0Frf6h72Pv2JD8/I5Num+jqhCF+4m60VwhUhNomdBGe9Y4XeJUBjOXZ6OEA3ak8NxomL5EKr0+P+WcbjJMRegcbgPhxaLvKx82Ytd/JGpskKgxt7LJ9HdJjs4IwxwEK0GnQYBzarHGKhJXVrENCwFW/qGxe6P/9Vo+XX+MIZdaNbhNaL9e9TH7/yhIwtlelIxO3w/4FAsmLrZP2mHtakFoozkXEJKZcjzkMbcqBaakMqWG8oZHD1MmlB8yCcZUpltfKJsX7MNW5Cr6IdFegbvZYLu53nsFW0iaAXpL53MMm4ciESoXfJ/cEAbad84jw==",
                "SigningCertUrl": "https://sns.us-east-2.amazonaws.com/SimpleNotificationService-01d088a6f77103d0fe307c0069e40ed6.pem",
                "UnsubscribeUrl": "https://sns.us-east-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results:7b8ea79b-8065-4587-8303-3128664d7615",
                "MessageAttributes": {}
            }
        }
    ]
}