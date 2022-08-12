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
import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.AvroDataObjects.ClickStreamEventAVRO;
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

public class ClickStreamDataReader
    extends PTransform<PCollection<QueryWindow>, PCollection<Event<ClickStreamEventAVRO>>> {

  @Override
  public PCollection<Event<ClickStreamEventAVRO>> expand(PCollection<QueryWindow> input) {

    RetailDemoStreamInjectorOptions options =
        input.getPipeline().getOptions().as(RetailDemoStreamInjectorOptions.class);
    String table = options.getClickStreamBQTable();

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

  public static class FetchClickStream extends DoFn<String, Event<ClickStreamEventAVRO>> {

    String project;

    public FetchClickStream(String project) {
      this.project = project;
    }

    @ProcessElement
    public void process(@Element String element, OutputReceiver<Event<ClickStreamEventAVRO>> o) {
      try {

        Long minTimeStamp = null;
        Long maxTimeStamp = null;

        TableResult result =
            BigQueryUtil.readDataFromBigQueryUsingQueryString(project, element, "InjectorReader");

        for (FieldValueList r : result.iterateAll()) {

          Event<ClickStreamEventAVRO> event = convertBQRowToClickStreamEvent(r);

          if (minTimeStamp == null || event.timestamp < minTimeStamp) {
            minTimeStamp = event.timestamp;
          }

          if (maxTimeStamp == null || event.timestamp > maxTimeStamp) {
            maxTimeStamp = event.timestamp;
          }

          o.output(event);
        }

        System.out.println(
            String.format(
                "ClickStream Min Timestamp %s Max Timestamp %s Current Time %s ",
                new Instant(minTimeStamp), new Instant(maxTimeStamp), Instant.now()));

      } catch (Exception ex) {
        // TODO ADD LOGGING
        ex.printStackTrace();
        System.out.println("ERROR ClickStream: " + ex.getMessage());
      }
    }
  }

  public static Event<ClickStreamEventAVRO> convertBQRowToClickStreamEvent(
      FieldValueList fieldValues) {

    Event<ClickStreamEventAVRO> event = new Event<>();
    ClickStreamEventAVRO clickEvent = new ClickStreamEventAVRO();

    event.data = clickEvent;

    // TODO check why no int in client
    clickEvent.uid = (fieldValues.get("uid").isNull()) ? 0 : fieldValues.get("uid").getLongValue();
    clickEvent.pageRef = fieldValues.get("page_ref").getStringValue();
    clickEvent.pageTarget = fieldValues.get("page_target").getStringValue();
    clickEvent.lat = fieldValues.get("lat").getDoubleValue();
    clickEvent.lng = fieldValues.get("lng").getDoubleValue();
    clickEvent.agent = fieldValues.get("agent").getStringValue();
    clickEvent.event = fieldValues.get("event").getStringValue();

    // Future date timestamp
    Instant futureTime =
        BQSQLUtil.switchDatePartOfTimeToToday(
            new Instant(fieldValues.get("timestamp").getTimestampValue() / 1000000000L));
    event.timestamp = futureTime.getMillis();
    clickEvent.timestamp = futureTime.getMillis();

    return event;
  }
}
