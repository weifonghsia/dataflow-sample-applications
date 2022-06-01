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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableMap;
import com.google.dataflow.sample.retail.pipeline.test.streamsimulator.retaildemo.core.Events;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.UnsupportedEncodingException;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetailDemoUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RetailDemoUtils.class);

  public static class ConvertEventAVROToPubSubMessage<T extends Events>
      extends DoFn<T, PubsubMessage> {

    Class clazz;
    Gson gson;

    public ConvertEventAVROToPubSubMessage(Class clazz) {
      this.clazz = clazz;
    }

    @Setup
    public void setup() {
      gson = new GsonBuilder().serializeNulls().create();
    }

    @ProcessElement
    public void process(@Element T input, OutputReceiver<PubsubMessage> o) {

      String json = gson.toJson(input, clazz);
      PubsubMessage pubsubMessage = null;
      try {
        pubsubMessage =
            new PubsubMessage(
                json.getBytes("UTF8"),
                ImmutableMap.of("TIMESTAMP", new Instant(input.timestamp).toString()));
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      }

      o.output(pubsubMessage);
    }
  }

  public static final boolean deleteTable(String project, String tableReference) {
    BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(project).build().getService();
    return bigquery.delete(tableReference);
  }

  public static final boolean createTableFromSource(
      String project,
      String sourceTable,
      String destinationDataasetId,
      String desinationTableId,
      @Nullable String jobIdPrefix)
      throws InterruptedException {

    BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(project).build().getService();

    String destinationTable =
        String.format("%s:%s.%s", project, destinationDataasetId, desinationTableId);

    String sqlStatement = String.format("SELECT * FROM %s WHERE timestamp < %s", destinationTable);

    Table desinationTable = bigquery.getTable(destinationDataasetId, desinationTableId);

    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(sqlStatement)
            .setDestinationTable(desinationTable.getTableId())
            .setUseLegacySql(false)
            .build();

    // Create a job ID so that we can safely retry.
    JobId jobId =
        JobId.of(
            String.format(
                "%s-%s",
                Optional.ofNullable(jobIdPrefix).orElse(""), UUID.randomUUID().toString()));

    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    // Wait for the query to complete.
    try {
      queryJob = queryJob.waitFor();
    } catch (InterruptedException e) {
      LOG.error("Exception while waiting for BigQuery Job result.", e);
      throw e;
    }

    // Check for errors
    if (queryJob == null) {
      throw new RuntimeException("Job no longer exists");
    } else if (queryJob.getStatus().getError() != null) {
      // You can also look at queryJob.getStatus().getExecutionErrors() for all
      // errors, not just the latest one.
      throw new RuntimeException(queryJob.getStatus().getError().toString());
    }
    TableResult result = null;
    try {
      result = queryJob.getQueryResults();
    } catch (InterruptedException e) {
      LOG.error("Exception while waiting for BigQuery Job result.", e);
    }

    return true;
  }
}
