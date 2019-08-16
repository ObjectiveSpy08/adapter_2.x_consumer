/**
 * Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * This file is licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License. A copy of
 * the License is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
*/


package com.amazonaws.samples;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

//import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.StreamSpecification;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public class StreamsAdapterDemoHelper {

    /**
     * @return StreamArn
     */
    public static String createTable(DynamoDbAsyncClient client, String tableName) {
        java.util.List<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
        attributeDefinitions.add(AttributeDefinition.builder().attributeName("Id").attributeType("N").build());

        java.util.List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
        keySchema.add(KeySchemaElement.builder().attributeName("Id").keyType(KeyType.HASH).build()); // Partition
                                                                                                 // key

        ProvisionedThroughput provisionedThroughput = ProvisionedThroughput.builder().readCapacityUnits(2L)
            .writeCapacityUnits(2L).build();

        StreamSpecification streamSpecification = StreamSpecification.builder().streamEnabled(true).streamViewType(StreamViewType.NEW_IMAGE).build();
//        streamSpecification.setStreamEnabled(true);
//        streamSpecification.setStreamViewType(StreamViewType.NEW_IMAGE);
        CreateTableRequest createTableRequest = CreateTableRequest.builder().tableName(tableName)
            .attributeDefinitions(attributeDefinitions).keySchema(keySchema)
            .provisionedThroughput(provisionedThroughput).streamSpecification(streamSpecification).build();

        try {
            System.out.println("Creating table " + tableName);
            CreateTableResponse result = client.createTable(createTableRequest).get();
            return result.tableDescription().latestStreamArn();
        }
        catch (ResourceInUseException e) {
            System.out.println("Table already exists!!!!!!!!!!!!!!!!!!!!!");
            CompletableFuture<DescribeTableResponse> resultFuture = describeTable(client, tableName);
            DescribeTableResponse result = null;
            try {
                result = resultFuture.get();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
                return null;
            } catch (ExecutionException ex) {
                ex.printStackTrace();
                return null;
            }
            return result.table().latestStreamArn();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        } catch (ExecutionException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static CompletableFuture<DescribeTableResponse> describeTable(DynamoDbAsyncClient client, String tableName) {
        return client.describeTable(DescribeTableRequest.builder().tableName(tableName).build());
    }

    public static ScanResponse scanTable(DynamoDbAsyncClient dynamoDBClient, String tableName) throws
        ExecutionException,
        InterruptedException {
        return dynamoDBClient.scan(ScanRequest.builder().tableName(tableName).build()).get();
    }

    public static void putItem(DynamoDbAsyncClient dynamoDBClient, String tableName, String id, String val) throws
        ExecutionException,
        InterruptedException {
        java.util.Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("Id", AttributeValue.builder().n(id).build());
        item.put("attribute-1", AttributeValue.builder().s(val).build());

        PutItemRequest putItemRequest = PutItemRequest.builder().tableName(tableName).item(item).build();
        dynamoDBClient.putItem(putItemRequest).get();
    }

    public static void putItem(DynamoDbAsyncClient dynamoDBClient, String tableName,
        java.util.Map<String, AttributeValue> items) throws ExecutionException, InterruptedException {
        PutItemRequest putItemRequest = PutItemRequest.builder().tableName(tableName).item(items).build();
        dynamoDBClient.putItem(putItemRequest).get();
    }

    public static void updateItem(DynamoDbAsyncClient dynamoDBClient, String tableName, String id, String val) throws
        ExecutionException,
        InterruptedException {
        java.util.Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put("Id", AttributeValue.builder().n(id).build());

        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<String, AttributeValueUpdate>();
        AttributeValueUpdate update = AttributeValueUpdate.builder().action(AttributeAction.PUT)
            .value(AttributeValue.builder().s(val).build()).build();
        attributeUpdates.put("attribute-2", update);

        UpdateItemRequest updateItemRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
            .attributeUpdates(attributeUpdates).build();
        dynamoDBClient.updateItem(updateItemRequest).get();
    }

    public static void deleteItem(DynamoDbAsyncClient dynamoDBClient, String tableName, String id) throws
        ExecutionException,
        InterruptedException {
        java.util.Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put("Id", AttributeValue.builder().n(id).build());

        DeleteItemRequest deleteItemRequest = DeleteItemRequest.builder().tableName(tableName).key(key).build();
        dynamoDBClient.deleteItem(deleteItemRequest).get();
    }

}

