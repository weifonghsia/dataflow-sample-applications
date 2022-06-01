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

import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.StreamInjectorOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

public interface RetailDemoStreamInjectorOptions
    extends DataflowPipelineOptions, StreamInjectorOptions {

  String getStaticDataProject();

  void setStaticDataProject(String staticDataProject);

  String getClickStreamBQTable();

  void setClickStreamBQTable(String clickStreamBQTable);

  String getTransactionBQTable();

  void setTransactionBQTable(String transactionBQTable);

  String getInventoryBQTable();

  void setInventoryBQTable(String InventoryBQTable);

  String getClickStreamPubSubOutput();

  void setClickStreamPubSubOutput(String clickStreamOutput);

  String getTransactionPubSubOutput();

  void setTransactionPubSubOutput(String transactionOutput);

  String getInventoryPubSubOutput();

  void setInventoryPubSubOutput(String inventoryOutput);

  String getClickStreamPubSubLowVolumeOutput();

  void setClickStreamPubSubLowVolumeOutput(String clickStreamLowVolumeOutput);

  String getTransactionPubSubLowVolumeOutput();

  void setTransactionPubSubLowVolumeOutput(String transactionLowVolumeOutput);

  String getInventoryPubSubLowVolumeOutput();

  void setInventoryPubSubLowVolumeOutput(String inventoryLowVolumeOutput);
}
