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

package co.cask.hydrator.transforms;


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
import co.cask.cdap.etl.common.StructuredRecordStringConverter;
import com.google.gson.Gson;

import java.io.IOException;
import java.util.List;

@Plugin(type = "transform")
@Name("JSONWriter")
@Description("Writes JSON from the Structured record.")
public class JSONWriter extends Transform<StructuredRecord, StructuredRecord> {
  private final Config config;
  private Gson gson;
  private Schema outSchema;

  public JSONWriter(Config config) {
    this.config = config;
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    gson = new Gson();
    try {
      outSchema = Schema.parseJson(config.schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Output Schema specified is not a valid JSON. Please check the Schema JSON");
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    try {
      Schema out = Schema.parseJson(config.schema);
      List<Schema.Field> fields = out.getFields();
      if (fields.size() > 1) {
        throw new IllegalArgumentException("Only one output field should exist for this transform and it should " +
                                             "ne of type String");  
      }
      
      if(fields.get(0).getSchema().getType() != Schema.Type.STRING) {
        throw new IllegalArgumentException("Output field name should be of type String. Please change type to " +
                                             "String");
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Output Schema specified is not a valid JSON. Please check the Schema JSON");
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord record = StructuredRecord.builder(outSchema)
      .set(outSchema.getFields().get(0).getName(), StructuredRecordStringConverter.toJsonString(input)).build();
    emitter.emit(record);
  }

  public static class Config extends PluginConfig {
    @Name("schema")
    @Description("Output schema")
    private String schema;


    public Config(String schema) {
      this.schema = schema;
    }

  }
}


