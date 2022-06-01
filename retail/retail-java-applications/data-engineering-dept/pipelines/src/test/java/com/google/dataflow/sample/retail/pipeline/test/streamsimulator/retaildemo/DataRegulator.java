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

import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.DataRegulator.QueryWindow;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.MutableDateTime;

/**
 * Generate the Window Interval that should be read from the past data sets. This should match the
 * impulse rate being created by the GenerateSequence.
 */
public class DataRegulator extends PTransform<PCollection<Long>, PCollection<QueryWindow>> {

  Duration duration;
  int yearOfStaticDataSource = 2019;
  Instant now;

  public DataRegulator(Duration duration, int yearOfStaticDataSource, Instant now) {
    this.duration = duration;
    this.yearOfStaticDataSource = yearOfStaticDataSource;
    this.now = now;
  }

  public DataRegulator(
      @Nullable String name, Duration duration, int yearOfStaticDataSource, Instant now) {
    super(name);
    this.duration = duration;
    this.yearOfStaticDataSource = yearOfStaticDataSource;
    this.now = now;
  }

  @Override
  public PCollection<QueryWindow> expand(PCollection<Long> input) {
    return input
        .apply(WithKeys.of(1))
        .apply(ParDo.of(new QueryWindowParameters(duration, yearOfStaticDataSource, now)));
  }

  public static class QueryWindowParameters extends DoFn<KV<Integer, Long>, QueryWindow> {

    Duration duration;
    int yearOfStaticDataSource = 2019;
    Instant now;

    public QueryWindowParameters(Duration duration, int yearOfStaticDataSource, Instant now) {
      this.duration = duration;
      this.yearOfStaticDataSource = yearOfStaticDataSource;
      this.now = now;
    }

    @StateId("queryWindowStart")
    private final StateSpec<ValueState<Long>> queryWindowStart =
        StateSpecs.value(VarLongCoder.of());

    @StateId("queryWindowEnd")
    private final StateSpec<ValueState<Long>> queryWindowEnd = StateSpecs.value(VarLongCoder.of());

    @ProcessElement
    public void process(
        OutputReceiver<QueryWindow> o,
        @StateId("queryWindowStart") ValueState<Long> queryWindowStart,
        @StateId("queryWindowEnd") ValueState<Long> queryWindowEnd) {

      Instant start;
      Instant end;

      if (queryWindowStart.read() == null) {
        start = now;
        end = now.plus(duration);

      } else {
        start = new Instant(queryWindowStart.read()).plus(duration);
        end = new Instant(queryWindowEnd.read()).plus(duration);
      }

      queryWindowStart.write(start.getMillis());
      queryWindowEnd.write(end.getMillis());

      MutableDateTime startAdjusted = start.toMutableDateTime();
      startAdjusted.setZone(DateTimeZone.UTC);

      startAdjusted.setWeekyear(start.toDateTime().getWeekyear());
      startAdjusted.setDayOfWeek(start.toDateTime().getDayOfWeek());
      startAdjusted.setYear(yearOfStaticDataSource);

      MutableDateTime endAdjusted = end.toMutableDateTime();
      endAdjusted.setZone(DateTimeZone.UTC);
      endAdjusted.setWeekyear(end.toDateTime().getWeekyear());
      endAdjusted.setDayOfWeek(end.toDateTime().getDayOfWeek());
      endAdjusted.setYear(yearOfStaticDataSource);

      QueryWindow window = new QueryWindow();
      window.queryWindowStart = startAdjusted.toInstant();
      window.queryWindowEnd = endAdjusted.toInstant();

      o.output(window);

      System.out.println(
          String.format(
              "Fetching Data from sources with data range %s to %s current time is %s",
              startAdjusted.toString(), endAdjusted.toString(), start.toString()));
    }
  }

  @DefaultCoder(AvroCoder.class)
  public static class QueryWindow {
    public Instant queryWindowStart;
    public Instant queryWindowEnd;
  }
}
