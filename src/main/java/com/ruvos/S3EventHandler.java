package com.ruvos;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.amazonaws.services.s3.event.S3EventNotification;

public class S3EventHandler implements RequestHandler<SQSEvent, String> {
    @Override
    public String handleRequest(SQSEvent event, Context context) {
        // We will only poll 1 message at a time, so .get(0) is fine
        SQSMessage message = event.getRecords().get(0);
        String messageBody = message.getBody();
        S3EventNotification s3EventNotification = S3EventNotification.parseJson(messageBody);
        S3EventNotification.S3EventNotificationRecord record = s3EventNotification.getRecords().get(0);

        String bucket = record.getS3().getBucket().getName();
        String key = record.getS3().getObject().getKey();

        context.getLogger().log("Bucket:" + bucket);
		context.getLogger().log("Key:" + key);

        // Note that if any exceptions are thrown, that the lambda will error
        // We WANT exceptions to throw so that when the lambda fails, the message will stay on the sqs queue
        // It will then retry, and if the configured number of retries fails, will go the an sqs dead letter queue
        return "success";
    }
}