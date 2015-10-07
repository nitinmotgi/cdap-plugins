/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */


package com.amazon;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import com.google.gson.Gson;

import java.io.IOException;
import java.util.List;

/**
 * ETL Transform.
 */

@Plugin(type = "transform")
@Name("JSONParser")
@Description("Parses JSON payload")
public class JSONParser extends Transform<StructuredRecord, StructuredRecord> {
  private final Config config;
  private Gson gson;
  private Schema outSchema;
  private List<Schema.Field> fields;

  public JSONParser(Config config) {
    this.config = config;
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    gson = new Gson();
    try {
      outSchema = Schema.parseJson(config.schema);
      fields = outSchema.getFields();
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of schema specified is invalid. Please check the format.");
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
  }

  private StructuredRecord createStructuredRecord(ScreenTime screenTime) {
    StructuredRecord.Builder builder = StructuredRecord.builder(outSchema);
    int i = 0;
    for(Schema.Field field : fields) {
      switch(field.getName()) {
        case "app_foreground_time":
          builder.set(field.getName(), screenTime.app_foreground_time);
          break;
        case "marketplace_id":
          builder.set(field.getName(), screenTime.marketplace_id);
          break;
        case "sqs_message_id":
          builder.set(field.getName(), screenTime.sqs_message_id);
          break;
        case "remote_addr":
          builder.set(field.getName(), screenTime.remote_addr);
          break;
        case "device_id":
          builder.set(field.getName(), screenTime.device_id);
          break;
        case "app_package_name":
          builder.set(field.getName(), screenTime.app_package_name);
          break;
        case "country_of_residence":
          builder.set(field.getName(), screenTime.country_of_residence);
          break;
        case "session":
          builder.set(field.getName(), screenTime.session);
          break;
        case "device_type":
          builder.set(field.getName(), screenTime.device_id);
          break;
        case "program":
          builder.set(field.getName(), screenTime.program);
          break;
        case "source":
          builder.set(field.getName(), screenTime.source);
          break;
        case "start_time_ms":
          builder.set(field.getName(), screenTime.start_time_ms);
          break;
        case "platform":
          builder.set(field.getName(), screenTime.platform);
          break;
        case "http_user_agent":
          builder.set(field.getName(), screenTime.http_user_agent);
          break;
        case "end_time_ms":
          builder.set(field.getName(), screenTime.end_time_ms);
          break;
        case "package_name":
          builder.set(field.getName(), screenTime.package_name);
          break;
        case "model":
          builder.set(field.getName(), screenTime.model);
          break;
        case "device_language":
          builder.set(field.getName(), screenTime.device_language);
          break;
        case "software_version":
          builder.set(field.getName(), screenTime.software_version);
          break;
        case "customer_id":
          builder.set(field.getName(), screenTime.customer_id);
          break;
        case "build_type":
          builder.set(field.getName(), screenTime.build_type);
          break;
        case "timestamp":
          builder.set(field.getName(), screenTime.timestamp);
          break;
        case "hardware":
          builder.set(field.getName(), screenTime.hardware);
          break;
      }
    }
    return builder.build();
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    String obj = input.get(config.field);
    ScreenTime screentime = gson.fromJson(obj, ScreenTime.class);
    emitter.emit(createStructuredRecord(screentime));
  }

  public static class Config extends PluginConfig {
    @Name("input")
    @Description("Input Field")
    private String field;

    @Name("schema")
    @Description("Output schema")
    private String schema;


    public Config(String field, String schema) {
      this.field = field;
      this.schema = schema;
    }

  }
}

