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
package com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.examples;

import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.DataCache;
import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.coders.EventCoder;
import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.datatypes.PipelineInternalDataTypes.Event;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Read data from a bounded source which contains the data we want to replay in stream mode. Future
 * date the data and load into our State Cache using State API. DataCache will using process time
 * triggers output data ever n Every n reload our cache with future data.
 */
public class ExampleStreamPipeline {

  public static void main(String[] args) throws Exception {
    // Setup Pipeline
    DataflowPipelineOptions options =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
    Pipeline p = Pipeline.create(options);

    // Start Generator to read our Data in
    PCollection<Event<Long>> event =
        p.apply(GenerateSequence.from(0).withRate(1, Duration.standardSeconds(20)))
            .apply(
                ParDo.of(
                    new DoFn<Long, Event<Long>>() {
                      @ProcessElement
                      public void process(ProcessContext pc) {
                        Event<Long> e = new Event<>();
                        e.data = pc.element();
                        // Future Date the data
                        e.timestamp = Instant.now().plus(Duration.standardSeconds(10)).getMillis();
                        pc.output(e);
                      }
                    }))
            .setCoder(new EventCoder<>(VarLongCoder.of()));

    // Load into DataCache
    event
        .apply(new DataCache<Long>())
        .apply(
            MapElements.into(TypeDescriptors.strings())
                .via(
                    (Long x) -> {
                      System.out.println(x);
                      return x.toString();
                    }));

    // Run pipeline
    p.run();
  }
}
