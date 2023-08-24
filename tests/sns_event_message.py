sns_event_message_textract_successful_job = {
    "Records": [
        {
            "EventSource": "aws:sns",
            "EventVersion": "1.0",
            "EventSubscriptionArn": "arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results:67e65130-6525-453b-a9ff-6f064b13545e",
            "Sns": {
                "Type": "Notification",
                "MessageId": "59e96349-a5cb-576b-978c-3e168f631b6a",
                "TopicArn": "arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results",
                "Subject": 'null',
                "Message": "{\"JobId\":\"822ec47a34fcad39e9b0b710bcf4ff78312dcb35347d3d4df3d17ca5442e31bc\",\"Status\":\"SUCCEEDED\",\"API\":\"StartDocumentAnalysis\",\"Timestamp\":1692739688904,\"DocumentLocation\":{\"S3ObjectName\":\"current/72_HR/BWI_72HR_18-20AUG23_b57c394d-0.pdf\",\"S3Bucket\":\"testing-ssa-pdf-store\"}}",
                "Timestamp": "2023-08-22T21:28:08.945Z",
                "SignatureVersion": "1",
                "Signature": "HRq7mlTUPK30nh8KPYiXQAJQ5mfeiffoXNgiPwg8Pn/rdpWM77H3ldupfVcsQpDZqmRbTk1EoeuH1dYHr4iHC/5G7uFemu8b/YGjwI/TXnYW/7dpiS7D+76Vfo+8Tcpdj95l8LolrpLQXsNEKfQNn1h8VfheLyVIvecRUEzbiNSAmjJIEVZPMjVcF6F6M+48mh+jnJMVsJBhrqp56kD8o0sqVMEjiRJUft8OhoNSLo6qV4yMKjCW0hEAeyO1kg5cIXkcqJbzPDmBfwCiuRy9cOjGzcMTUlsbtV0sVac9pIBDQtlh6b3QTHvlrtc2L/zB4NXJ/1m8BLIFkXcgFi3GoA==",
                "SigningCertUrl": "https://sns.us-east-2.amazonaws.com/SimpleNotificationService-01d088a6f77103d0fe307c0069e40ed6.pem",
                "UnsubscribeUrl": "https://sns.us-east-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results:67e65130-6525-453b-a9ff-6f064b13545e",
                "MessageAttributes": {}
            }
        }
    ]
}

sns_event_message_textract_failed_job = {
    "Records": [
        {
            "EventSource": "aws:sns",
            "EventVersion": "1.0",
            "EventSubscriptionArn": "arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results:67e65130-6525-453b-a9ff-6f064b13545e",
            "Sns": {
                "Type": "Notification",
                "MessageId": "59e96349-a5cb-576b-978c-3e168f631b6a",
                "TopicArn": "arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results",
                "Subject": 'null',
                "Message": "{\"JobId\":\"822ec47a34fcad39e9b0b710bcf4ff78312dcb35347d3d4df3d17ca5442e31bc\",\"Status\":\"FAILED\",\"API\":\"StartDocumentAnalysis\",\"Timestamp\":1692739688904,\"DocumentLocation\":{\"S3ObjectName\":\"current/72_HR/BWI_72HR_18-20AUG23_b57c394d-0.pdf\",\"S3Bucket\":\"testing-ssa-pdf-store\"}}",
                "Timestamp": "2023-08-22T21:28:08.945Z",
                "SignatureVersion": "1",
                "Signature": "HRq7mlTUPK30nh8KPYiXQAJQ5mfeiffoXNgiPwg8Pn/rdpWM77H3ldupfVcsQpDZqmRbTk1EoeuH1dYHr4iHC/5G7uFemu8b/YGjwI/TXnYW/7dpiS7D+76Vfo+8Tcpdj95l8LolrpLQXsNEKfQNn1h8VfheLyVIvecRUEzbiNSAmjJIEVZPMjVcF6F6M+48mh+jnJMVsJBhrqp56kD8o0sqVMEjiRJUft8OhoNSLo6qV4yMKjCW0hEAeyO1kg5cIXkcqJbzPDmBfwCiuRy9cOjGzcMTUlsbtV0sVac9pIBDQtlh6b3QTHvlrtc2L/zB4NXJ/1m8BLIFkXcgFi3GoA==",
                "SigningCertUrl": "https://sns.us-east-2.amazonaws.com/SimpleNotificationService-01d088a6f77103d0fe307c0069e40ed6.pem",
                "UnsubscribeUrl": "https://sns.us-east-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results:67e65130-6525-453b-a9ff-6f064b13545e",
                "MessageAttributes": {}
            }
        }
    ]
}

sns_event_message_textract_error_job = {
    "Records": [
        {
            "EventSource": "aws:sns",
            "EventVersion": "1.0",
            "EventSubscriptionArn": "arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results:67e65130-6525-453b-a9ff-6f064b13545e",
            "Sns": {
                "Type": "Notification",
                "MessageId": "59e96349-a5cb-576b-978c-3e168f631b6a",
                "TopicArn": "arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results",
                "Subject": 'null',
                "Message": "{\"JobId\":\"822ec47a34fcad39e9b0b710bcf4ff78312dcb35347d3d4df3d17ca5442e31bc\",\"Status\":\"ERROR\",\"API\":\"StartDocumentAnalysis\",\"Timestamp\":1692739688904,\"DocumentLocation\":{\"S3ObjectName\":\"current/72_HR/BWI_72HR_18-20AUG23_b57c394d-0.pdf\",\"S3Bucket\":\"testing-ssa-pdf-store\"}}",
                "Timestamp": "2023-08-22T21:28:08.945Z",
                "SignatureVersion": "1",
                "Signature": "HRq7mlTUPK30nh8KPYiXQAJQ5mfeiffoXNgiPwg8Pn/rdpWM77H3ldupfVcsQpDZqmRbTk1EoeuH1dYHr4iHC/5G7uFemu8b/YGjwI/TXnYW/7dpiS7D+76Vfo+8Tcpdj95l8LolrpLQXsNEKfQNn1h8VfheLyVIvecRUEzbiNSAmjJIEVZPMjVcF6F6M+48mh+jnJMVsJBhrqp56kD8o0sqVMEjiRJUft8OhoNSLo6qV4yMKjCW0hEAeyO1kg5cIXkcqJbzPDmBfwCiuRy9cOjGzcMTUlsbtV0sVac9pIBDQtlh6b3QTHvlrtc2L/zB4NXJ/1m8BLIFkXcgFi3GoA==",
                "SigningCertUrl": "https://sns.us-east-2.amazonaws.com/SimpleNotificationService-01d088a6f77103d0fe307c0069e40ed6.pem",
                "UnsubscribeUrl": "https://sns.us-east-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results:67e65130-6525-453b-a9ff-6f064b13545e",
                "MessageAttributes": {}
            }
        }
    ]
}
