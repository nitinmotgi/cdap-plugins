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
@Description("Encodes field values using one of the digest algorithms. MD2, MD5, SHA1, SHA256, SHA384 and SHA512 are " +
  "the supported message digest algorithms.")
public class Hasher extends Transform<StructuredRecord, StructuredRecord> {
  private final Config config;
  private String[] fieldsToHash;

  public Hasher(Config config) {
    this.config = config;
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    
    // Split the fields to be hashed.
    fieldsToHash = config.fields.split(",");
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    
    // Checks if hash specified is one of the supported types. 
    if(!config.hash.equalsIgnoreCase("md2") && !config.hash.equalsIgnoreCase("md5") &&
        !config.hash.equalsIgnoreCase("sha1") && !config.hash.equalsIgnoreCase("sha256") &&
        !config.hash.equalsIgnoreCase("sha384") && !config.hash.equalsIgnoreCase("sha512")) {
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
        String value = in.get(name);
        String digest = value;
        switch(config.hash.toLowerCase()) {
          case "md2":
            digest = DigestUtils.md2Hex(value);
            break;
          case "md5":
            digest = DigestUtils.md5Hex(value);
            break;
          case "sha1":
            digest = DigestUtils.sha1Hex(value);
            break;
          case "sha256":
            digest = DigestUtils.sha256Hex(value);
            break;
          case "sha384":
            digest = DigestUtils.sha384Hex(value);
            break;
          case "sha512":
            digest = DigestUtils.sha256Hex(value);
            break;
        }
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
