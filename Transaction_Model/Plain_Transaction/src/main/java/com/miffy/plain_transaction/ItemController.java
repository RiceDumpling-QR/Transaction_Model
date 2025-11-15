package com.miffy.plain_transaction;

import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;


import org.springframework.web.bind.annotation.*;
import software.amazon.awssdk.services.dynamodb.*;
import software.amazon.awssdk.services.dynamodb.model.*;
import java.util.*;

@RestController
@RequestMapping("/items")
@CrossOrigin(origins = "*")
public class ItemController {

    private final DynamoDbClient dynamoDb;
    private final String tableName = "Transactions";

    public ItemController() {
        this.dynamoDb = DynamoDbClient.builder()
                .region(software.amazon.awssdk.regions.Region.US_EAST_1)
                .build();
    }

    @PostMapping
    public Map<String, Object> saveItem(@RequestBody Item body) {

        String id = UUID.randomUUID().toString();

        PutItemRequest req = PutItemRequest.builder()
                .tableName(tableName)
                .item(Map.of(
                        "purchase_id", AttributeValue.fromS(id),
                        "item", AttributeValue.fromS(body.item),
                        "quantity", AttributeValue.fromN(String.valueOf(body.quantity))
                ))
                .build();

        dynamoDb.putItem(req);

        return Map.of(
                "purchase_id", id,
                "item", body.item,
                "quantity", body.quantity
        );
    }
}
