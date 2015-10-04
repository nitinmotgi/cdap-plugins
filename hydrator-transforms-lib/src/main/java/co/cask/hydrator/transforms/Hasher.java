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
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.codec.digest.DigestUtils;

import java.util.List;
import java.util.Random;
import javax.annotation.Nullable;

@Plugin(type = "transform")
@Name("Hasher")
@Description("Masker masks string fields. Mask generated are of same length as the input field value.")
public class Hasher extends Transform<StructuredRecord, StructuredRecord> {
  private final Config config;
  private String[] fieldsToHash;
  
  ImmutableMap<String, Boolean> allowedHash = ImmutableMap.<String, Boolean>builder()
    .put("md2", true)
    .put("md5", true)
    .put("sha1", true)
    .put("sha256", true)
    .put("sha384", true)
    .put("sha512", true)
    .build();

  public Hasher(Config config) {
    this.config = config;
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    fieldsToHash = config.fields.split(",");
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    
    if(!allowedHash.containsKey(config.hash.toLowerCase())) {
      throw new IllegalArgumentException("Invalid hasher '" + config.hash + "' specified. Allowed hashers are md2, " +
                                           "md5, sha1, sha256, sha384 and sha512");  
    }
  }

  @Override
  public void transform(StructuredRecord in, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord.Builder builder = StructuredRecord.builder(in.getSchema());
    
    List<Schema.Field> fields = in.getSchema().getFields();
    for(Schema.Field field : fields) {
      String name = field.getName();
      if (okToHash(name) && field.getSchema().getType() == Schema.Type.STRING) {
        String digest = hashTheField((String) in.get(name));
        builder.set(name, digest);
      } else {
        builder.set(name, in.get(name));
      }
    }
    emitter.emit(builder.build());
  }

  private boolean okToHash(String name) {
    for(String field : fieldsToHash){
      if (name.equalsIgnoreCase(field)) {
        return true;
      }
    }
    return false;
  }


  private String hashTheField (String value) {
    String hash = config.hash.toLowerCase();
    switch(hash) {
      case "md2":
        return DigestUtils.md2Hex(value);
      case "md5":
        return DigestUtils.md5Hex(value);
      case "sha1":
        return DigestUtils.sha1Hex(value);
      case "sha256":
        return DigestUtils.sha256Hex(value);
      case "sha384":
        return DigestUtils.sha384Hex(value);
      case "sha512":
        return DigestUtils.sha256Hex(value);
    }
    // We should never get here.
    return value;
  }


  public static class Config extends PluginConfig {
    @Name("hash")
    @Description("Specifies the Hash method for hashing fields.")
    @Nullable
    private final String hash;
    
    @Name("fields")
    @Description("List of fields to hash. Only string fields are allowed")
    private final String fields;
    
    public Config(String hash, String fields) {
      this.hash = hash;
      this.fields = fields;
    }
    
  }
}
