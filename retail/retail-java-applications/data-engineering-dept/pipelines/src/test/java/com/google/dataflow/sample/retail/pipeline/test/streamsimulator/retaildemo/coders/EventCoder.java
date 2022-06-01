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
package com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.coders;

import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.datatypes.PipelineInternalDataTypes.Event;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;

public class EventCoder<V> extends StructuredCoder<Event<V>> {

  private final Coder<V> dataCoder;

  public EventCoder(Coder<V> dataCoder) {
    this.dataCoder = dataCoder;
  }

  public Coder<V> getDataCoder() {
    return dataCoder;
  }

  @Override
  public void encode(Event<V> value, OutputStream outStream) throws CoderException, IOException {

    if (value == null) {
      throw new CoderException("cannot encode a null");
    }

    if (value.timestamp == null) {
      throw new CoderException("cannot encode a null timestamp");
    }

    VarLongCoder.of().encode(value.timestamp, outStream);
    NullableCoder.of(getDataCoder()).encode(value.data, outStream);
  }

  @Override
  public Event<V> decode(InputStream inStream) throws CoderException, IOException {
    Event<V> event = new Event<>();
    event.timestamp = VarLongCoder.of().decode(inStream);
    event.data = NullableCoder.of(getDataCoder()).decode(inStream);

    return event;
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Arrays.asList(this.dataCoder);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {

    Coder.verifyDeterministic(this, "data coder must be deterministic", this.dataCoder);
  }

  @Override
  public TypeDescriptor<Event<V>> getEncodedTypeDescriptor() {
    return (new TypeDescriptor<Event<V>>() {})
        .where(new TypeParameter<V>() {}, this.getDataCoder().getEncodedTypeDescriptor());
  }
}
