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
import co.cask.cdap.api.data.schema.Schema.Field;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import com.google.common.collect.Maps;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Decodes the input fields as BASE64, BASE32 or HEX.
 */
@Plugin(type = "transform")
@Name("Decoder")
@Description("Decodes the input field(s) using Base64, Base32 or Hex")
public class Decoder extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(Decoder.class);
  private final Config config;

  // Output Schema associated with transform output.
  private Schema outSchema;

  // Mapping of input field to decoder type.
  private final Map<String, EncodeDecodeType> decodeMap = Maps.newTreeMap();

  // Decoder handlers.
  private final Base64 base64Decoder = new Base64();
  private final Base32 base32Decoder = new Base32();
  private final Hex hexDecoder = new Hex();

  // Output Field name to type map
  private Map<String, Schema.Type> outSchemaMap = Maps.newHashMap();

  // This is used only for tests, otherwise this is being injected by the ingestion framework.
  public Decoder(Config config) {
    this.config = config;
  }
  
  private void parseConfiguration(String foo) throws IllegalArgumentException {
    String[] mappings = config.decode.split(",");
    for(String mapping : mappings) {
      String[] params = mapping.split(":");
      
      // If format is not right, then we throw an exception.
      if(params.length < 2) {
        throw new IllegalArgumentException("Configuration " + mapping + " is in-correctly formed. " +
                                             "Format should be <fieldname>:<decode-type>");
      }
      
      String field = params[0];
      String type = params[1].toUpperCase();
      EncodeDecodeType eType = EncodeDecodeType.BASE64;
      
      switch(type) {
        case "BASE64":
          eType = EncodeDecodeType.BASE64;
          break;
        
        case "BASE32":
          eType = EncodeDecodeType.BASE32;
          break;
        
        case "HEX":
          eType = EncodeDecodeType.HEX;
          break;
        
        case "NONE":
          eType = EncodeDecodeType.NONE;
          break;
        
        default:
          throw new IllegalArgumentException("Unknown decode type " + type + " found in mapping " + mapping);
      }
      
      if(decodeMap.containsKey(field)) {
        throw new IllegalArgumentException("Field " + field + " already has decoder set. Check the mapping.");
      } else {
        decodeMap.put(field, eType);
      }
    }
  }
  
  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    parseConfiguration(config.decode);
    try {
      outSchema = Schema.parseJson(config.schema);
      List<Field> outFields = outSchema.getFields();
      for(Field field : outFields) {
        outSchemaMap.put(field.getName(), field.getSchema().getType());  
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of schema specified is invalid. Please check the format.");
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    parseConfiguration(config.decode);
    // Check if schema specified is a valid schema or no. 
    try {
      Schema.parseJson(config.schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of schema specified is invalid. Please check the format.");
    }
  }
  

  @Override
  public void transform(StructuredRecord in, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord.Builder builder = StructuredRecord.builder(outSchema);
    
    Schema inSchema = in.getSchema();
    List<Field> inFields = inSchema.getFields();
    
    // Iterate through input fields. Check if field name is present 
    // in the fields that need to be decoded, if it's not then write
    // to output as it is. 
    for(Field field : inFields) {
      String name = field.getName();
      
      // Check if output schema also have the same field name. If it's not 
      // then throw an exception. 
      if(!outSchemaMap.containsKey(name)) {
        throw new Exception("Field " + name + " is not defined in the output.");  
      }
      
      Schema.Type outFieldType = outSchemaMap.get(name);
      
      // Check if the input field name is configured to be decoded. If the field is not
      // present or is defined as none, then pass through the field as is. 
      if(!decodeMap.containsKey(name) || decodeMap.get(name) == EncodeDecodeType.NONE) {
        builder.set(name, in.get(name));          
      } else {
        // Now, the input field could be of type String or byte[], so transform everything
        // to byte[] 
        byte[] obj = new byte[0];
        if(field.getSchema().getType() == Schema.Type.STRING) {
          obj = ((String)in.get(name)).getBytes();  
        } else if (field.getSchema().getType() == Schema.Type.BYTES){
          obj = in.get(name);
        }
        
        // Now, based on the decode type configured for the field - decode the byte[] of the
        // value.
        byte[] outValue = new byte[0];
        EncodeDecodeType type = decodeMap.get(name);
        if (type == EncodeDecodeType.BASE32) {
          outValue = base32Decoder.decode(obj);
        } else if (type == EncodeDecodeType.BASE64) {
          outValue = base64Decoder.decode(obj);
        } else if (type == EncodeDecodeType.HEX) {
          outValue = hexDecoder.decode(obj);
        }
        
        // Depending on the output field type, either convert it to 
        // Bytes or to String. 
        if(outFieldType == Schema.Type.BYTES) {
          builder.set(name, outValue);
        } else if (outFieldType == Schema.Type.STRING) {
          builder.set(name, new String(outValue));
        }
      }
    }
    emitter.emit(builder.build());
  }

  public static class Config extends PluginConfig {
    @Name("decode")
    @Description("Specify the field and decode type combination. " +
      "Format is <field>:<decode-type>[,<field>:<decode-type>]*. ")
    private final String decode;

    @Name("schema")
    @Description("Specifies the output schema")
    private final String schema;
    
    public Config(String decode, String schema) {
      this.decode = decode;
      this.schema = schema;
    }
  }
}

