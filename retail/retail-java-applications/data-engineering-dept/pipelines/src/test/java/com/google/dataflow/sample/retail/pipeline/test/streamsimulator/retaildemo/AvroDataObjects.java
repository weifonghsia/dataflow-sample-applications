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
package com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo;

import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.core.Events;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

public class AvroDataObjects {

  @DefaultCoder(AvroCoder.class)
  public static class ClickStreamEventAVRO extends Events {
    public @org.apache.avro.reflect.Nullable Long uid;
    public @org.apache.avro.reflect.Nullable String sessionId;
    public @org.apache.avro.reflect.Nullable boolean returning;
    public @org.apache.avro.reflect.Nullable String pageRef;
    public @org.apache.avro.reflect.Nullable String pageTarget;
    public @org.apache.avro.reflect.Nullable Double lat;
    public @org.apache.avro.reflect.Nullable Double lng;
    public @org.apache.avro.reflect.Nullable String agent;
    public @org.apache.avro.reflect.Nullable String event;
    public @org.apache.avro.reflect.Nullable boolean transaction;
  }

  /** A transaction is a purchase, either in-store or via the website / mobile application. */
  @DefaultCoder(AvroCoder.class)
  public static class TransactionsAvro extends Events {
    public @org.apache.avro.reflect.Nullable int uid;
    public @org.apache.avro.reflect.Nullable String order_number;
    public @org.apache.avro.reflect.Nullable int user_id;
    public @org.apache.avro.reflect.Nullable int store_id;
    public @org.apache.avro.reflect.Nullable boolean returning;
    public @org.apache.avro.reflect.Nullable long time_of_sale;
    public @org.apache.avro.reflect.Nullable int department_id;
    public @org.apache.avro.reflect.Nullable int product_id;
    public @org.apache.avro.reflect.Nullable int product_count;
    public @org.apache.avro.reflect.Nullable float price;

    // Not populated at Point of sale
    public @org.apache.avro.reflect.Nullable int order_id;
    // Not populated at Point of sale
    public @org.apache.avro.reflect.Nullable int order_dow;
    // Not populated at Point of sale
    public @org.apache.avro.reflect.Nullable int order_hour_of_day;
    // Not populated at Point of sale
    public @org.apache.avro.reflect.Nullable int order_woy;
    // Not populated at Point of sale
    public @org.apache.avro.reflect.Nullable String days_since_prior_order;
    // Not populated at Point of sale
    public @org.apache.avro.reflect.Nullable String product_name;
    // Not populated at Point of sale
    public @org.apache.avro.reflect.Nullable int product_sku;
    public @org.apache.avro.reflect.Nullable String image;
  }

  @DefaultCoder(AvroCoder.class)
  public static class InventoryAVRO extends Events {

    public @org.apache.avro.reflect.Nullable int count;
    public @org.apache.avro.reflect.Nullable int sku;
    public @org.apache.avro.reflect.Nullable int productId;
    public @org.apache.avro.reflect.Nullable int storeId;
    public @org.apache.avro.reflect.Nullable int aisleId;
    public @org.apache.avro.reflect.Nullable String product_name;
    public @org.apache.avro.reflect.Nullable int departmentId;
    public @org.apache.avro.reflect.Nullable Float price;
    public @org.apache.avro.reflect.Nullable String recipeId;
    public @org.apache.avro.reflect.Nullable String image;
  }
}
