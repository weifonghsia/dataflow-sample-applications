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

import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.MutableDateTime;

/** TODO convert this to Splittable DoFn when it becomes available */
public class BQSQLUtil {

  public static Instant switchDatePartOfTimeToToday(Instant timestamp) {

    Instant now = Instant.now();

    MutableDateTime mutable = new Instant(timestamp).toMutableDateTime();
    mutable.setZone(DateTimeZone.UTC);
    mutable.setYear(now.get(DateTimeFieldType.year()));
    mutable.setDayOfYear(now.get(DateTimeFieldType.dayOfYear()));

    return mutable.toInstant();
  }
}
