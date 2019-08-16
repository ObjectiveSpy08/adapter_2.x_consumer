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
//import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;

//import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.processor.ShardRecordProcessor;

//import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;

//import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import software.amazon.kinesis.lifecycle.events.InitializationInput;

//import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;

//import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;


import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;

//import com.amazonaws.services.kinesis.model.Record;
import software.amazon.services.dynamodb.streamsadapter.model.RecordObjectMapper;
import software.amazon.services.dynamodb.streamsadapter.processor.DynamoDBStreamsShardRecordProcessor;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

//public class StreamsRecordProcessor implements IRecordProcessor {
public class StreamsRecordProcessor implements DynamoDBStreamsShardRecordProcessor {
    private Integer checkpointCounter;

    private final DynamoDbAsyncClient dynamoDBClient;
    private final String tableName;

    public StreamsRecordProcessor(DynamoDbAsyncClient dynamoDBClient2, String tableName) {
        this.dynamoDBClient = dynamoDBClient2;
        this.tableName = tableName;
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        checkpointCounter = 0;
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput, List<Record> records) {
        System.out.println("Processing records");
        records.forEach((record) -> {
            try {
                StreamsAdapterDemoHelper.putItem(dynamoDBClient, tableName, record.dynamodb().newImage().get("Id").n(), record.dynamodb().newImage().get("attribute-1").s());
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //                System.out.println(rec);
        });
//        ObjectMapper mapper = new RecordObjectMapper();
//        mapper.registerModule(new JavaTimeModule());
        //mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//        processRecordsInput.records().forEach((record) -> {
//            try {
//                byte[] arr = new byte[record.data().remaining()];
//                record.data().get(arr);
//                software.amazon.awssdk.services.dynamodb.model.Record rec = mapper.readValue(arr, software.amazon.awssdk.services.dynamodb.model.Record.serializableBuilderClass()).build();
//                //StreamsAdapterDemoHelper.putItem(dynamoDBClient, tableName, rec.dynamodb().newImage());
//                StreamsAdapterDemoHelper.putItem(dynamoDBClient, tableName, rec.dynamodb().newImage().get("Id").n(), rec.dynamodb().newImage().get("attribute-1").s());
//                System.out.println(rec);
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } catch (ExecutionException e) {
//                e.printStackTrace();
//            }
//        });
//        for (Record record : processRecordsInput.records()) {
//            String data = new String(record.getData().array(), Charset.forName("UTF-8"));
//            System.out.println(data);
//
//            if (record instanceof RecordAdapter) {
//                com.amazonaws.services.dynamodbv2.model.Record streamRecord = ((RecordAdapter) record)
//                        .getInternalObject();
//
//                // switch (streamRecord.getEventName()) {
//                //     case "INSERT":
//                //     case "MODIFY":
//                //         StreamsAdapterDemoHelper.putItem(dynamoDBClient, tableName,
//                //                                          streamRecord.getDynamodb().getNewImage());
//                //         break;
//                //     case "REMOVE":
//                //         StreamsAdapterDemoHelper.deleteItem(dynamoDBClient, tableName,
//                //                                             streamRecord.getDynamodb().getKeys().get("Id").getN());
//                // }
//                if (streamRecord.getEventName() == "INSERT" || streamRecord.getEventName() == "MODIFY") {
//                        StreamsAdapterDemoHelper.putItem(dynamoDBClient, tableName,
//                                                         streamRecord.getDynamodb().getNewImage());
//                } else if (streamRecord.getEventName() == "REMOVE") {
//                        StreamsAdapterDemoHelper.deleteItem(dynamoDBClient, tableName,
//                                                            streamRecord.getDynamodb().getKeys().get("Id").getN());
//                }
//            }
//            checkpointCounter += 1;
//            if (checkpointCounter % 10 == 0) {
//                try {
//                    processRecordsInput.getCheckpointer().checkpoint();
//                }
//                catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        }
        
    }

    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
        System.out.println("Lease lost.");
    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
        System.out.println("Shard ended");
        try {
            shardEndedInput.checkpointer().checkpoint();
        } catch (ShutdownException | InvalidStateException e) {
            //
            // Swallow the exception
            //
            e.printStackTrace();
        }
    }

    // @Override
    // public void shutdown(ShutdownInput shutdownInput) {
    //     if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
    //         try {
    //             shutdownInput.getCheckpointer().checkpoint();
    //         }
    //         catch (Exception e) {
    //             e.printStackTrace();
    //         }
    //     }  
    // }    

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        try {
            shutdownRequestedInput.checkpointer().checkpoint();
        } catch (ShutdownException | InvalidStateException e) {
            //
            // Swallow the exception
            //
            e.printStackTrace();
        }
    }
}

