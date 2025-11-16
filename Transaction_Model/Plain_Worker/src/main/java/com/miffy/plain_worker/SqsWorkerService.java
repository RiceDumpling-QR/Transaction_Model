package com.miffy.plain_worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.miffy.plain_worker.model.Item;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Service;
import org.springframework.scheduling.annotation.Scheduled;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

@Service
public class SqsWorkerService {

    private static final Logger log = LoggerFactory.getLogger(SqsWorkerService.class);

    private final SqsClient sqs = SqsClient.builder()
            .region(Region.US_EAST_1)
            .build();

    private final DynamoDbClient dynamo = DynamoDbClient.builder()
            .region(Region.US_EAST_1)
            .build();

    private final ObjectMapper mapper = new ObjectMapper();

    private final String queueUrl =
            "https://sqs.us-east-1.amazonaws.com/463000836942/transaction_queue";

    private final String tableName = "Transactions";

    private final Queue<Item> buffer = new ConcurrentLinkedQueue<>();

    @PostConstruct
    public void startPolling() {
        Thread t = new Thread(() -> {
            log.info("SQS worker started, polling queue...");
            while (true) {
                try {
                    ReceiveMessageResponse res = sqs.receiveMessage(
                            ReceiveMessageRequest.builder()
                                    .queueUrl(queueUrl)
                                    .maxNumberOfMessages(10)
                                    .waitTimeSeconds(10)
                                    .build()
                    );

                    if (!res.messages().isEmpty()) {
                        log.info("Received {} messages from SQS", res.messages().size());
                    }

                    for (Message msg : res.messages()) {
                        Item item = mapper.readValue(msg.body(), Item.class);
                        buffer.add(item);

                        sqs.deleteMessage(DeleteMessageRequest.builder()
                                .queueUrl(queueUrl)
                                .receiptHandle(msg.receiptHandle())
                                .build());
                    }

                    Thread.sleep(1000);

                } catch (Exception e) {
                    log.error("Error in worker loop", e);
                    try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
                }
            }
        });

        t.setDaemon(false);
        t.start();
    }
    
    @Scheduled(fixedRate = 30000)
    public void flushToDynamo() {
        List<Item> batch = new ArrayList<>();
        while (!buffer.isEmpty()) {
            Item it = buffer.poll();
            if (it != null) batch.add(it);
        }

        if (batch.isEmpty()) {
            log.info("No items to flush this round.");
            return;
        }

        log.info("Flushing {} items to DynamoDB...", batch.size());

        for (Item it : batch) {
            PutItemRequest req = PutItemRequest.builder()
                    .tableName(tableName)
                    .item(Map.of(
                            "purchase_id", AttributeValue.fromS(it.purchase_id),
                            "item", AttributeValue.fromS(it.item),
                            "quantity", AttributeValue.fromN(String.valueOf(it.quantity))
                    ))
                    .build();

            dynamo.putItem(req);
        }

        log.info("Flush complete.");
    }
}
