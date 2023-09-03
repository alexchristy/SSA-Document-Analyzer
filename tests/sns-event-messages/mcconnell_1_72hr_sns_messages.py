mcconnell_1_72hr_successful_job_sns_message = {
    "Records": [
        {
            "EventSource": "aws:sns",
            "EventVersion": "1.0",
            "EventSubscriptionArn": "arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results:7b8ea79b-8065-4587-8303-3128664d7615",
            "Sns": {
                "Type": "Notification",
                "MessageId": "db7da769-0bf7-5654-8109-fa0790ed22b2",
                "TopicArn": "arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results",
                "Subject": 'null',
                "Message": "{\"JobId\":\"0f274545b9307046ed8cb5042e55629414c1eccb2fc65df9fcf6ac9c59ff7f73\",\"Status\":\"SUCCEEDED\",\"API\":\"StartDocumentAnalysis\",\"Timestamp\":1693751514916,\"DocumentLocation\":{\"S3ObjectName\":\"current/72_HR/McConnell_72HR_15AUG23_c1ac3943-3.pdf\",\"S3Bucket\":\"testing-ssa-pdf-store\"}}",
                "Timestamp": "2023-09-03T14:31:54.971Z",
                "SignatureVersion": "1",
                "Signature": "VB103ZTo6/SQcScXDLCJbMrkguuWcXxsPUOJCN0CmWq30PRRo4Q93IwpBR20/+qzBedg6IiwUwkLNGq/SIAruFj5KwpZJBpWbBlWfa6rLj1YdZNFZdzglh97WoBBLXlzapH9TzAPJFj2eSHNZeTixE8+r9Q2hpvYvtp1Cd7Q6M0OmcnasnMyOziTYgUHTu74csa3DyvtIHe7V28fqlwPcol/jFH4dQvOEoOoj5JQ2MNGy77udHLZz3hmhd8hctsocV4VTZwrYJ7TuTvURYZk/pc8yPPn9poiZrM6Yg8h3v70dqd0pT2RyE+iktqskEa0v1V1aOvitatCiuUE1VFozg==",
                "SigningCertUrl": "https://sns.us-east-2.amazonaws.com/SimpleNotificationService-01d088a6f77103d0fe307c0069e40ed6.pem",
                "UnsubscribeUrl": "https://sns.us-east-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results:7b8ea79b-8065-4587-8303-3128664d7615",
                "MessageAttributes": {}
            }
        }
    ]
}
