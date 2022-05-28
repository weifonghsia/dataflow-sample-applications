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

import com.google.devrel.dataflow.demo.retail.businesslogic.utils.DataObjectUtils;
import com.google.devrel.dataflow.demo.retail.schemas.business.ClickStream.ClickStreamEventAVRO;
import com.google.devrel.dataflow.streamsimulator.datatypes.PipelineInternalDataTypes.Event;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class ClickstreamErrorGenerator
    extends PTransform<PBegin, PCollection<Event<ClickStreamEventAVRO>>> {

  @Override
  public PCollection<Event<ClickStreamEventAVRO>> expand(PBegin input) {

    return input
        .apply(GenerateSequence.from(0).withRate(1, Duration.standardSeconds(10)))
        .apply(
            ParDo.of(
                new DoFn<Long, Event<ClickStreamEventAVRO>>() {

                  @ProcessElement
                  public void process(ProcessContext pc) {

                    Instant now = Instant.now();
                    ClickStreamEventAVRO clilckErrorEvent = new ClickStreamEventAVRO();
                    clilckErrorEvent.timestamp = now.getMillis();
                    clilckErrorEvent.pageRef = "P200";
                    clilckErrorEvent.pageTarget = "P375";
                    clilckErrorEvent.lng = 43.726075;
                    clilckErrorEvent.lng = -71.642508;
                    clilckErrorEvent.agent =
                        "Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148";
                    clilckErrorEvent.transaction = false;
                    clilckErrorEvent.uid = 1L;
                    clilckErrorEvent.returning = false;
                    clilckErrorEvent.event = "ERROR-CODE-B87769A";

                    int clickOutputCount = (now.toDateTime().getHourOfDay() == 10) ? 1 : 10;

                    for (int i = 0; i < clickOutputCount; i++) {
                      Event<ClickStreamEventAVRO> event1 = new Event<>();
                      event1.timestamp = clilckErrorEvent.timestamp;
                      event1.data = clilckErrorEvent;

                      pc.output(event1);
                    }

                    ClickStreamEventAVRO missingUID = DataObjectUtils.copyAVRO(clilckErrorEvent);
                    missingUID.uid = null;
                    missingUID.sessionId = ThreadLocalRandom.current().nextLong();
                    Event<ClickStreamEventAVRO> event2 = new Event<>();
                    event2.timestamp = missingUID.timestamp;
                    event2.data = missingUID;

                    pc.output(event2);

                    ClickStreamEventAVRO missingLatLng = DataObjectUtils.copyAVRO(clilckErrorEvent);
                    Event<ClickStreamEventAVRO> event3 = new Event<>();
                    event3.timestamp = missingLatLng.timestamp;
                    event3.data = missingLatLng;

                    pc.output(event3);
                  }
                }));
  }
}
