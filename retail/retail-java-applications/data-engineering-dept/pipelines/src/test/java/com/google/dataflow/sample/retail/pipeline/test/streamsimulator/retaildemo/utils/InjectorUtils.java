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
package com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.utils;

import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.DataRegulator.QueryWindow;

public class InjectorUtils {

  public static String getQuery(String table, QueryWindow queryWindow) {

    String query =
        "SELECT\n"
            + "  *\n"
            + "FROM\n"
            + "`"
            + table
            + "`\n"
            + "WHERE\n"
            + "  timestamp BETWEEN TIMESTAMP(\""
            + queryWindow.queryWindowStart.toString()
            + "\")\n"
            + "  AND TIMESTAMP(\""
            + queryWindow.queryWindowEnd.toString()
            + "\")";

    System.out.println(query);
    return query;
  }

  //  public static class ConvertEventAVROToPubSubMessage<T extends Events>
  //      extends DoFn<T, PubsubMessage> {
  //
  //    Class clazz;
  //    Gson gson;
  //
  //    public ConvertEventAVROToPubSubMessage(Class clazz) {
  //      this.clazz = clazz;
  //    }
  //
  //    @Setup
  //    public void setup() {
  //      gson = new GsonBuilder().serializeNulls().create();
  //    }
  //
  //    @ProcessElement
  //    public void process(@Element T input, OutputReceiver<PubsubMessage> o) {
  //
  //      String json = gson.toJson(input, clazz);
  //      PubsubMessage pubsubMessage = null;
  //      try {
  //        pubsubMessage =
  //            new PubsubMessage(
  //                json.getBytes("UTF8"),
  //                ImmutableMap.of("TIMESTAMP", new Instant(input.timestamp).toString()));
  //      } catch (UnsupportedEncodingException e) {
  //        e.printStackTrace();
  //      }
  //
  //      o.output(pubsubMessage);
  //    }
  //  }
}
