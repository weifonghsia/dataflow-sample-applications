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

import com.google.cloud.bigquery.FieldValueList;
import com.google.gson.JsonObject;

/** Limited utility class that takes a flat row and outputs it as a JSON Object */
public class FieldValueListToJSONObject {

  public static JsonObject convertFiledValueListToJson(FieldValueList list) {

    //        TableResult result;
    //        FieldValueList aa;
    //
    //        JsonObject object = new JsonObject();
    //        list.
    //        for (FieldValue f : list) {
    //
    //
    //            switch (f.getAttribute()) {
    //                case PRIMITIVE:
    //                    Object o;
    //                    if(f.getLongValue()){
    //                        o=f.getLongValue();
    //                    }
    //                    if(f.getBooleanValue()!=null){
    //                        o=f.getBooleanValue();
    //                    }
    //
    //                    if(f.getBytesValue()!=null){
    //                        o=f.getBytesValue();
    //                    }
    //
    //                    if(Optional.of(f.getLongValue()).isPresent()){
    //                        o=f.getLongValue();
    //                    }
    //
    //                    if(Optional.of(f.getLongValue()).isPresent()){
    //                        o=f.getLongValue();
    //                    }
    //
    //                    if(Optional.of(f.getLongValue()).isPresent()){
    //                        o=f.getLongValue();
    //                    }
    //
    //                case REPEATED:
    //                case RECORD:
    //            }
    //        }

    return null;
  }
}
