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

//import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

//import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.kinesis.processor.ShardRecordProcessor;

// import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.services.dynamodb.streamsadapter.processor.DynamoDBStreamsShardRecordProcessor;
import software.amazon.services.dynamodb.streamsadapter.processor.DynamoDBStreamsShardRecordProcessorFactory;

//public class StreamsRecordProcessorFactory implements IRecordProcessorFactory {
public class StreamsRecordProcessorFactory implements DynamoDBStreamsShardRecordProcessorFactory {
    private final String tableName;
    private final DynamoDbAsyncClient dynamoDBClient;

    public StreamsRecordProcessorFactory(DynamoDbAsyncClient dynamoDBClient, String tableName) {
        this.tableName = tableName;
        this.dynamoDBClient = dynamoDBClient;
    }

//    @Override
//    public ShardRecordProcessor shardRecordProcessor() {
//    //public IRecordProcessor createProcessor() {
//        return new StreamsRecordProcessor(dynamoDBClient, tableName);
//    }
    public DynamoDBStreamsShardRecordProcessor shardRecordProcessor() {
        return new StreamsRecordProcessor(dynamoDBClient, tableName);
    }
}
