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
package com.google.devrel.dataflow.streamsimulator.retaildemo.customflows;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class ClickstreamBrokenJSONGenerator extends PTransform<PBegin, PCollection<PubsubMessage>> {

  @Override
  public PCollection<PubsubMessage> expand(PBegin input) {

    return input
        .apply(GenerateSequence.from(0).withRate(1, Duration.standardSeconds(10)))
        .apply(
            ParDo.of(
                new DoFn<Long, PubsubMessage>() {

                  @ProcessElement
                  public void process(ProcessContext pc) {
                    PubsubMessage message =
                        new PubsubMessage(
                            "{I AM NOT A JSON I AM A FREE PERSON!}".getBytes(),
                            ImmutableMap.of("TIMESTAMP", Instant.now().toString()));
                    pc.output(message);
                  }
                }));
  }
}
