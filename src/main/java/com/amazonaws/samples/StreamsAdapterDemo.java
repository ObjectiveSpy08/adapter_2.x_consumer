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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.UUID;
import java.util.concurrent.*;

//import com.amazonaws.auth.AWSCredentialsProvider;
//import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

//import com.amazonaws.regions.Regions;
import org.apache.commons.lang3.RandomStringUtils;
import software.amazon.awssdk.regions.Region;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import org.slf4j.MDC;
//import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
//import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
//import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
//import com.amazonaws.services.dynamodbv2.streamsadapter.StreamsWorkerFactory;

import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;
import software.amazon.services.dynamodb.streamsadapter.*;

import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.services.dynamodb.streamsadapter.utils.DynamoDBStreamsClientUtil;

// import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
//import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
//import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
//import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

public class StreamsAdapterDemo {
    private static final Logger log = LoggerFactory.getLogger(StreamsAdapterDemo.class);

    private static Scheduler scheduler;

    private static String tablePrefix = "KCL-Demo";
    private static String streamArn;

    //private static Region region = Region.of("us-west-2");

   // private static AWSCredentialsProvider awsCredentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();

    private static DynamoDbAsyncClient dynamoDBClient;
    private static CloudWatchAsyncClient cloudWatchClient;
    private static DynamoDbStreamsAsyncClient dynamoDBStreamsClient;
    private static KinesisAsyncClient kinesisClient;

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        System.out.println("Starting application...");

//        dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
//            .withRegion(awsRegion)
//            .build();
//        cloudWatchClient = AmazonCloudWatchClientBuilder.standard()
//            .withRegion(awsRegion)
//            .build();
//        dynamoDBStreamsClient = AmazonDynamoDBStreamsClientBuilder.standard()
//            .withRegion(awsRegion)
//            .build();
//        adapterClient = new AmazonDynamoDBStreamsAdapterClient(dynamoDBStreamsClient);

        dynamoDBStreamsClient = DynamoDBStreamsClientUtil.createDynamoDbStreamsAsyncClient(DynamoDbStreamsAsyncClient.builder().region(Region.US_WEST_2));

        dynamoDBClient = DynamoDbAsyncClient.builder().region(Region.US_WEST_2).build();
        cloudWatchClient = CloudWatchAsyncClient.builder().region(Region.US_WEST_2).build();
        //dynamoDBStreamsClient = DynamoDbStreamsAsyncClient.builder().region(Region.US_WEST_2).build();
        kinesisClient = new DynamoDBStreamsAsyncClientAdapter(dynamoDBStreamsClient);

        String srcTable = tablePrefix + "-src";
        String destTable = tablePrefix + "-dest";
        //recordProcessorFactory = new StreamsRecordProcessorFactory(dynamoDBClient, destTable);

        setUpTables(dynamoDBClient);
        Thread.sleep(20000);
        ScheduledExecutorService producerExecutor = Executors.newSingleThreadScheduledExecutor();
        ScheduledFuture<?> producerFuture = producerExecutor.scheduleAtFixedRate(() -> {
            try {
                performOps(srcTable);
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, 10, 4, TimeUnit.SECONDS);
        //ConfigsBuilder configsBuilder = new ConfigsBuilder(streamArn, "streams-adapter-demo", kinesisClient, dynamoDBClient, cloudWatchClient, UUID.randomUUID().toString(), new StreamsRecordProcessorFactory(dynamoDBClient, destTable));
        ConfigsBuilder configsBuilder = DynamoDBStreamsFactory.createConfigsBuilder(streamArn, "streams-adapter-demo", kinesisClient, dynamoDBClient, cloudWatchClient, UUID.randomUUID().toString(), new StreamsRecordProcessorFactory(dynamoDBClient, destTable));
//        workerConfig = new KinesisClientLibConfiguration("streams-adapter-demo",
//            streamArn,
//            awsCredentialsProvider,
//            "streams-demo-worker")
//            .withMaxRecords(1000)
//            .withIdleTimeBetweenReadsInMillis(500)
//            .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);

        System.out.println("Creating a scheduler for stream: " + streamArn);
        scheduler = DynamoDBStreamsFactory.createScheduler(configsBuilder);
        System.out.println("Starting the scheduler...");
//        Thread t = new Thread(scheduler);
//        t.start();

        Thread schedulerThread = new Thread(scheduler);
        schedulerThread.setDaemon(true);
        schedulerThread.start();

        //Thread.sleep(25000);
        //System.out.println("Press enter to shutdown");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        try {
            reader.readLine();
        } catch (IOException ioex) {
            log.error("Caught exception while waiting for confirm. Shutting down.", ioex);
        }
        //scheduler.shutdown();
        producerFuture.cancel(true);
        producerExecutor.shutdownNow();
        Future<Boolean> gracefulShutdownFuture = scheduler.startGracefulShutdown();


        try {
            gracefulShutdownFuture.get(20, TimeUnit.SECONDS);
            //scheduler.shutdown();

            //schedulerThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            log.info("Interrupted while waiting for graceful shutdown. Continuing.");
            System.out.println("Interrupted!!!!!!!");
        }

//        if (StreamsAdapterDemoHelper.scanTable(dynamoDBClient, srcTable).items()
//            .equals(StreamsAdapterDemoHelper.scanTable(dynamoDBClient, destTable).items())) {
//            System.out.println("Scan result is equal.");
//        }
//        else {
//            System.out.println("Tables are different!");
//        }

        System.out.println("Done.");
        //System.exit(0);
        Runtime.getRuntime().halt(0);
        //cleanupAndExit(0);
    }

    private static void setUpTables(DynamoDbAsyncClient dynamoDBClient) {
        String srcTable = tablePrefix + "-src";
        String destTable = tablePrefix + "-dest";
        streamArn = StreamsAdapterDemoHelper.createTable(dynamoDBClient, srcTable);

        StreamsAdapterDemoHelper.createTable(dynamoDBClient, destTable);

        try {
            awaitTableCreation(srcTable);
            //performOps(srcTable);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void awaitTableCreation(String tableName) throws ExecutionException, InterruptedException {
        System.out.println("Awaiting table and stream creation");
        Integer retries = 0;
        Boolean created = false, streamEnabled;
        while (!created && retries < 100) {
            CompletableFuture<DescribeTableResponse> resultFuture = StreamsAdapterDemoHelper.describeTable(dynamoDBClient, tableName);
            DescribeTableResponse result = resultFuture.get();
            created = result.table().tableStatusAsString().equals("ACTIVE");
            streamEnabled = result.table().streamSpecification().streamEnabled();
            if (created && streamEnabled) {
                //System.out.println("Table and stream are active.");
                return;
            }
            else {
                retries++;
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e) {
                    // do nothing
                    e.printStackTrace();
                }
            }
        }
        System.out.println("Timeout after table creation. Exiting...");
        //cleanupAndExit(1);
    }

    private static void performOps(String tableName) throws ExecutionException, InterruptedException {
        String id1, id2;
        id1 = RandomStringUtils.randomNumeric(4);
        id2 = RandomStringUtils.randomNumeric(4);
        StreamsAdapterDemoHelper.putItem(dynamoDBClient, tableName, id1, "original1");
        //StreamsAdapterDemoHelper.updateItem(dynamoDBClient, tableName, id1, "updated1");
        //StreamsAdapterDemoHelper.deleteItem(dynamoDBClient, tableName, "101");
        StreamsAdapterDemoHelper.putItem(dynamoDBClient, tableName, id2, "original2");
        //StreamsAdapterDemoHelper.updateItem(dynamoDBClient, tableName, id2, "updated2");
        //StreamsAdapterDemoHelper.deleteItem(dynamoDBClient, tableName, "102");
    }

    private static void cleanupAndExit(Integer returnValue) {
        String srcTable = tablePrefix + "-src";
        String destTable = tablePrefix + "-dest";
        dynamoDBClient.deleteTable(DeleteTableRequest.builder().tableName(srcTable).build());
        dynamoDBClient.deleteTable(DeleteTableRequest.builder().tableName(destTable).build());
        System.exit(returnValue);
    }
}
