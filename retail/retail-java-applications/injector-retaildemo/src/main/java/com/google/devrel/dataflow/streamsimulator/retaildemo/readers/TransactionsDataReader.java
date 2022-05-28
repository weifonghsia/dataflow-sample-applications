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
package com.google.devrel.dataflow.streamsimulator.retaildemo.readers;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableMap;
import com.google.devrel.dataflow.demo.retail.businesslogic.core.utils.BigQueryUtil;
import com.google.devrel.dataflow.demo.retail.schemas.business.Transaction.TransactionsAvro;
import com.google.devrel.dataflow.streamsimulator.DataRegulator.QueryWindow;
import com.google.devrel.dataflow.streamsimulator.datatypes.PipelineInternalDataTypes.Event;
import com.google.devrel.dataflow.streamsimulator.retaildemo.pipelines.RetailDemoStreamInjectorOptions;
import com.google.devrel.dataflow.streamsimulator.utils.BQSQLUtil;
import com.google.devrel.dataflow.streamsimulator.utils.InjectorUtils;
import com.google.gson.Gson;
import java.io.UnsupportedEncodingException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Instant;

public class TransactionsDataReader
    extends PTransform<PCollection<QueryWindow>, PCollection<Event<TransactionsAvro>>> {

  @Override
  public PCollection<Event<TransactionsAvro>> expand(PCollection<QueryWindow> input) {
    String table =
        input
            .getPipeline()
            .getOptions()
            .as(RetailDemoStreamInjectorOptions.class)
            .getTransactionBQTable();
    return input
        .apply(
            MapElements.into(TypeDescriptors.strings())
                .via(
                    x -> {
                      return InjectorUtils.getQuery(table, x);
                    }))
        .apply(ParDo.of(new FetchClickStream()));
  }

  public static class FetchClickStream extends DoFn<String, Event<TransactionsAvro>> {

    @ProcessElement
    public void process(@Element String input, OutputReceiver<Event<TransactionsAvro>> o) {
      try {

        Long minTimeStamp = null;
        Long maxTimeStamp = null;

        TableResult result =
            BigQueryUtil.readDataFromBigQueryUsingQueryString(
                "instant-insights", input, "StreamRetail");

        for (FieldValueList r : result.iterateAll()) {

          Event<TransactionsAvro> event = convertBQRowToTransactions(r);

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
                "Transactions Min Timestamp %s Max Timestamp %s Current Time %s ",
                new Instant(minTimeStamp), new Instant(maxTimeStamp), Instant.now()));

      } catch (Exception ex) {
        // TODO ADD LOGGING
        ex.printStackTrace();
        System.out.println("ERROR Transactions: " + ex);
      }
    }
  }

  public static class ConvertClickAVROToPubSubMessage
      extends DoFn<TransactionsAvro, PubsubMessage> {

    @ProcessElement
    public void process(@Element TransactionsAvro input, OutputReceiver<PubsubMessage> o) {

      Gson gson = new Gson();
      String json = gson.toJson(input, TransactionsAvro.class);
      PubsubMessage pubsubMessage = null;
      try {
        pubsubMessage =
            new PubsubMessage(
                json.getBytes("UTF8"),
                ImmutableMap.of("Timestamp", new Instant(input.timestamp).toString()));
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      }

      o.output(pubsubMessage);
    }
  }

  public static Event<TransactionsAvro> convertBQRowToTransactions(FieldValueList fieldValues) {

    Event<TransactionsAvro> event = new Event<>();
    TransactionsAvro transactions = new TransactionsAvro();

    event.data = transactions;

    // TODO check why no int in client
    // TODO Wrap all in NULLABLE

    transactions.uid =
        (fieldValues.get("uid").isNull())
            ? 0
            : Integer.valueOf((String) fieldValues.get("uid").getValue());

    // transactions.returning = fieldValues.get("returning").getBooleanValue();
    transactions.order_number = fieldValues.get("order_number").getStringValue();
    transactions.user_id =
        (fieldValues.get("user_id").isNull())
            ? 0
            : Integer.valueOf((String) fieldValues.get("user_id").getValue());

    // .get().getNumericValue().intValue();
    //    transactions.order_id = fieldValues.get("order_id").getNumericValue().intValue();
    //    transactions.order_dow = fieldValues.get("order_dow").getNumericValue().intValue();
    //    transactions.order_hour_of_day =
    //        fieldValues.get("order_hour_of_day").getNumericValue().intValue();

    //    transactions.days_since_prior_order =
    // (days_since_prior_order!=null)?days_since_prior_order.getStringValue():"0";

    //    transactions.order_woy = fieldValues.get("order_woy").getNumericValue().intValue();
    //    transactions.time_of_sale = fieldValues.get("time_of_sale").getNumericValue().intValue();
    transactions.timestamp = fieldValues.get("timestamp").getNumericValue().intValue();
    transactions.store_id = fieldValues.get("store_id").getNumericValue().intValue();
    //    transactions.product_name = fieldValues.get("product_name").getStringValue();

    transactions.product_count = fieldValues.get("product_count").getNumericValue().intValue();
    //    transactions.product_sku = fieldValues.get("product_sku").getNumericValue().intValue();
    transactions.product_id = fieldValues.get("product_id").getNumericValue().intValue();
    transactions.price = fieldValues.get("price").getNumericValue().floatValue();
    //    transactions.department_id =
    // fieldValues.get("department_id").getNumericValue().intValue();
    //    transactions.image = fieldValues.get("image").getStringValue();

    // Future date timestamp
    Instant futureTime =
        BQSQLUtil.switchDatePartOfTimeToToday(
            new Instant(fieldValues.get("timestamp").getTimestampValue() / 1000L));
    event.timestamp = futureTime.getMillis();
    transactions.timestamp = futureTime.getMillis();

    return event;
  }
}
