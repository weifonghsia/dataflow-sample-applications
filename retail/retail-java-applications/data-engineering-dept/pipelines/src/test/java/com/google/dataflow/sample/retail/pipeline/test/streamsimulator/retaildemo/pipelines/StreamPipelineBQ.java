/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.pipelines;

import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.AvroDataObjects.ClickStreamEventAVRO;
import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.AvroDataObjects.InventoryAVRO;
import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.AvroDataObjects.TransactionsAvro;
import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.DataCache;
import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.DataRegulator;
import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.DataRegulator.QueryWindow;
import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.coders.EventCoder;
import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.customflows.ClickstreamBrokenJSONGenerator;
import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.customflows.ClickstreamErrorGenerator;
import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.datatypes.PipelineInternalDataTypes.Event;
import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.readers.ClickStreamDataReader;
import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.readers.InventoryDataReader;
import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.readers.TransactionsDataReader;
import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.utils.RetailDemoUtils.ConvertEventAVROToPubSubMessage;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Read data from a bounded source which contains the data we want to replay in stream mode. Future
 * date the data and load into our State Cache using State API. DataCache will using process time
 * triggers output data ever n Every n reload our cache with future data.
 */
public class StreamPipelineBQ {

  /** */
  public static void deleteDestinationTableData(String destinationTable) {}

  public static void copySourceTableFromBeforeTimestamp(String sourceTable, Instant readTime) {}

  public static void main(String[] args) throws Exception {
    // Setup Pipeline
    RetailDemoStreamInjectorOptions options =
        PipelineOptionsFactory.create().as(RetailDemoStreamInjectorOptions.class);
    Pipeline p = Pipeline.create(options);

    options.setRunner(DataflowRunner.class);
    // options.setRegion("us-east1");
    // options.setMaxNumWorkers(4);
    // options.setExperiments(ImmutableList.of("min_num_workers=2", "enable_streaming_engine"));

    // Starting point for the streaming data and the historical data load
    Instant now = Instant.now();

    // Delete the old tables

    // Start Generator to read our Data inputs

    PCollection<QueryWindow> queryWindow =
        p.apply(GenerateSequence.from(0).withRate(1, Duration.standardMinutes(30)))
            .apply(new DataRegulator(Duration.standardMinutes(30), 2020, now));

    // Load and process Clickstream DataCache
    PCollection<Event<ClickStreamEventAVRO>> clickstream =
        queryWindow
            .apply(new ClickStreamDataReader())
            .setCoder(new EventCoder<>(AvroCoder.of(ClickStreamEventAVRO.class)));

    PCollection<Event<ClickStreamEventAVRO>> eventCreated =
        p.apply(new ClickstreamErrorGenerator())
            .setCoder(new EventCoder<>(AvroCoder.of(ClickStreamEventAVRO.class)));

    PCollection<Event<ClickStreamEventAVRO>> event =
        PCollectionList.of(eventCreated)
            .and(clickstream)
            .apply(Flatten.<Event<ClickStreamEventAVRO>>pCollections());

    PCollection<PubsubMessage> clickStreamtoPubsubMessage =
        event
            .apply(new DataCache<ClickStreamEventAVRO>())
            .apply(
                ParDo.of(
                    new ConvertEventAVROToPubSubMessage<ClickStreamEventAVRO>(
                        ClickStreamEventAVRO.class)));

    // Load and process Transactions DataCache
    PCollection<Event<TransactionsAvro>> transactionEvents =
        queryWindow
            .apply(new TransactionsDataReader())
            .setCoder(new EventCoder<>(AvroCoder.of(TransactionsAvro.class)));

    PCollection<PubsubMessage> transactionToPubsubMessage =
        transactionEvents
            .apply(new DataCache<TransactionsAvro>())
            .apply(
                ParDo.of(
                    new ConvertEventAVROToPubSubMessage<TransactionsAvro>(TransactionsAvro.class)));

    // Load and process inventory DataCache
    PCollection<Event<InventoryAVRO>> inventoryEvents =
        queryWindow
            .apply(new InventoryDataReader())
            .setCoder(new EventCoder<>(AvroCoder.of(InventoryAVRO.class)));

    PCollection<PubsubMessage> inventoryToPubsubMessage =
        inventoryEvents
            .apply(new DataCache<InventoryAVRO>())
            .apply(
                ParDo.of(new ConvertEventAVROToPubSubMessage<InventoryAVRO>(InventoryAVRO.class)));

    // Push data into PubSub
    clickStreamtoPubsubMessage.apply(
        PubsubIO.writeMessages().to(options.getClickStreamPubSubOutput()));

    // Push data into PubSub
    p.apply(new ClickstreamBrokenJSONGenerator())
        .apply(PubsubIO.writeMessages().to(options.getClickStreamPubSubOutput()));

    // Push data into PubSub
    transactionToPubsubMessage.apply(
        PubsubIO.writeMessages().to(options.getTransactionPubSubOutput()));

    // Push data into PubSub
    inventoryToPubsubMessage.apply(PubsubIO.writeMessages().to(options.getInventoryPubSubOutput()));

    // Low volume output for notebooks

    p.run();
  }
}
