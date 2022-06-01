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

import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.datatypes.PipelineInternalDataTypes.Event;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * MapState is not available on all runners, although there are performance measures we could take
 * in order to make this more efficent with BagState for v0.1 we will go with naive implementation.
 */
public class DataCache<V> extends PTransform<PCollection<Event<V>>, PCollection<V>> {

  int buckets = 100;

  public DataCache() {}

  public DataCache(@Nullable String name) {
    super(name);
  }

  public DataCache(int buckets) {
    this.buckets = buckets;
  }

  public DataCache(@Nullable String name, int buckets) {
    super(name);
    this.buckets = buckets;
  }

  @Override
  public PCollection<V> expand(PCollection<Event<V>> input) {
    // State used to hold all right stream values

    return input
        .apply(
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.longs(), new TypeDescriptor<Event<V>>() {}))
                .via(x -> KV.of(getBucket(), x)))
        .setCoder(KvCoder.of(VarLongCoder.of(), input.getCoder()))
        .apply(ParDo.of(new FutureCache<Long, V>(input.getCoder())));
  }

  public Long getBucket() {
    Long value = ThreadLocalRandom.current().nextLong(100);
    return value;
  }
  /**
   * Future dated Timestamped data points will be placed in the bag Processing Time timer is set to
   * fire when the option is set Everything in the bag behind the current time is sent out. If there
   * is nothing left the Processing Time timer does not reset itself
   */
  public static class FutureCache<K, V> extends DoFn<KV<K, Event<V>>, V> {

    Coder<Event<V>> objectCoder;

    @TimerId("Process")
    private final TimerSpec processTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @StateId("cache")
    private final StateSpec<BagState<Event<V>>> cache;

    public FutureCache(Coder<Event<V>> objectCoder) {
      this.objectCoder = objectCoder;
      cache = StateSpecs.bag(objectCoder);
    }

    @ProcessElement
    public void process(
        @TimerId("Process") Timer timer,
        @StateId("cache") BagState<Event<V>> cache,
        @Element KV<K, Event<V>> input) {
      cache.add(input.getValue());
      // Will just keep updating for each element.
      timer.offset(Duration.standardSeconds(1)).setRelative();
    }

    @OnTimer("Process")
    public void OnTimer(
        @TimerId("Process") Timer timer,
        OnTimerContext otc,
        @StateId("cache") BagState<Event<V>> cache) {

      // Cycle through all events with timestamp less than wall clock time and emit
      Instant impulseTimestamp = Instant.now();

      Iterator<Event<V>> events = cache.read().iterator();
      List<Event<V>> remainingEvents = new ArrayList<>();

      // Naive impl, just reload the bag each time
      // TODO replace with MapState when available in all runners
      while (events.hasNext()) {
        Event<V> event = events.next();
        if (event.timestamp <= impulseTimestamp.getMillis()) {
          otc.output(event.data);
        } else {
          remainingEvents.add(event);
        }
      }

      cache.clear();

      if (!remainingEvents.isEmpty()) {
        remainingEvents.forEach(x -> cache.add(x));
        timer
            .offset(
                Duration.standardSeconds(
                    otc.getPipelineOptions()
                        .as(StreamInjectorOptions.class)
                        .getOutPutImpulseDurationSeconds()))
            .setRelative();
      }
    }
  }
}
