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
package com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.readers;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.dataflow.sample.retail.businesslogic.core.utils.BigQueryUtil;
import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.AvroDataObjects.InventoryAVRO;
import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.DataRegulator.QueryWindow;
import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.datatypes.PipelineInternalDataTypes.Event;
import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.pipelines.RetailDemoStreamInjectorOptions;
import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.utils.BQSQLUtil;
import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.utils.InjectorUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Instant;

public class InventoryDataReader
    extends PTransform<PCollection<QueryWindow>, PCollection<Event<InventoryAVRO>>> {
  @Override
  public PCollection<Event<InventoryAVRO>> expand(PCollection<QueryWindow> input) {

    RetailDemoStreamInjectorOptions options =
        input.getPipeline().getOptions().as(RetailDemoStreamInjectorOptions.class);
    String table = options.getInventoryBQTable();

    String project = options.getProject();
    return input
        .apply(
            MapElements.into(TypeDescriptors.strings())
                .via(
                    x -> {
                      return InjectorUtils.getQuery(table, x);
                    }))
        .apply(ParDo.of(new FetchClickStream(project)));
  }

  public static class FetchClickStream extends DoFn<String, Event<InventoryAVRO>> {

    String project;

    public FetchClickStream(String project) {
      this.project = project;
    }

    @ProcessElement
    public void process(@Element String input, OutputReceiver<Event<InventoryAVRO>> o) {
      try {

        Long minTimeStamp = null;
        Long maxTimeStamp = null;

        TableResult result =
            BigQueryUtil.readDataFromBigQueryUsingQueryString(project, input, "InventoryInjector");

        for (FieldValueList r : result.iterateAll()) {

          Event<InventoryAVRO> event = convertBQRowToInventory(r);

          if (minTimeStamp == null || event.timestamp < minTimeStamp) {
            minTimeStamp = event.timestamp;
          }

          if (maxTimeStamp == null || event.timestamp > maxTimeStamp) {
            maxTimeStamp = event.timestamp;
          }

          o.output(event);
        }

        // Inject one dummy event per every cycle to show some activity on channel.
        // TODO remove this once all testing is complete
        InventoryAVRO avro = new InventoryAVRO();
        avro.count = 1;
        avro.productId = 10050;
        avro.storeId = 99999;
        avro.timestamp = Instant.now().getMillis();

        Event dummy = new Event();
        dummy.data = avro;
        dummy.timestamp = avro.timestamp;

        o.output(dummy);

        System.out.println(dummy);

        System.out.println(
            String.format(
                "Inventory Min Timestamp %s Max Timestamp %s Current Time %s ",
                new Instant(minTimeStamp), new Instant(maxTimeStamp), Instant.now()));

      } catch (Exception ex) {
        // TODO ADD LOGGING
        System.out.println("ERROR Inventory: " + ex.getMessage());
      }
    }
  }

  public static Event<InventoryAVRO> convertBQRowToInventory(FieldValueList fieldValues) {

    Event<InventoryAVRO> event = new Event<>();
    InventoryAVRO inventoryEvent = new InventoryAVRO();

    event.data = inventoryEvent;

    // TODO check why no int in client
    inventoryEvent.count = Integer.valueOf((String) fieldValues.get("count").getValue());
    inventoryEvent.productId = Integer.valueOf((String) fieldValues.get("productId").getValue());
    inventoryEvent.storeId = Integer.valueOf((String) fieldValues.get("storeId").getValue());

    fieldValues.get("page_ref").getStringValue();

    // Future date timestamp
    Instant futureTime =
        BQSQLUtil.switchDatePartOfTimeToToday(
            new Instant(fieldValues.get("timestamp").getTimestampValue() / 1000L));
    event.timestamp = futureTime.getMillis();
    inventoryEvent.timestamp = futureTime.getMillis();

    return event;
  }
}
