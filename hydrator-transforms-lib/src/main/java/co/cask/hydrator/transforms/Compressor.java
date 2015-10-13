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
import org.xerial.snappy.Snappy;
import sun.misc.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.Inflater;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * Compresses configured fields using the algorithms specified.
 */
@Plugin(type = "transform")
@Name("Compressor")
@Description("Compresses configured fields using the algorithms specified.")
public final class Compressor extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(Compressor.class);
  private final Config config;

  // Output Schema associated with transform output.
  private Schema outSchema;

  // Output Field name to type map
  private Map<String, Schema.Type> outSchemaMap = Maps.newHashMap();

  private final Map<String, CompDecompType> compMap = Maps.newTreeMap();

  // This is used only for tests, otherwise this is being injected by the ingestion framework.
  public Compressor(Config config) {
    this.config = config;
  }
  
  private void parseConfiguration(String config) throws IllegalArgumentException {
    String[] mappings = config.split(",");
    for(String mapping : mappings) {
      String[] params = mapping.split(":");
      
      // If format is not right, then we throw an exception.
      if(params.length < 2) {
        throw new IllegalArgumentException("Configuration " + mapping + " is in-correctly formed. " +
                                             "Format should be <fieldname>:<compressor-type>");
      }
      
      String field = params[0];
      String type = params[1].toUpperCase();
      CompDecompType cType = CompDecompType.NONE;

      switch(type) {
        case "SNAPPY":
          cType = CompDecompType.SNAPPY;
          break;
        
        case "GZIP":
          cType = CompDecompType.GZIP;
          break;
        
        case "ZIP":
          cType = CompDecompType.ZIP;
          break;

        case "NONE":
          cType = CompDecompType.NONE;
          break;
        
        default:
          throw new IllegalArgumentException("Unknown encoder type " + type + " found in mapping " + mapping);
      }
      if(compMap.containsKey(field)) {
        throw new IllegalArgumentException("Field " + field + " already has compressor set. Check the mapping.");
      } else {
        compMap.put(field, cType);
      }
    }
  }
  
  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    parseConfiguration(config.compressor);
    try {
      outSchema = Schema.parseJson(config.schema);
      List<Field> outFields = outSchema.getFields();
      for(Field field : outFields) {
        outSchemaMap.put(field.getName(), field.getSchema().getType());  
      }
      
      for(String field : compMap.keySet()) {
        if(compMap.containsKey(field)) {
          Schema.Type type = outSchemaMap.get(field);
          if (type != Schema.Type.BYTES) {
            throw new IllegalArgumentException("Field '" + field + "' is not of type BYTES. It's currently" +
                                                 "of type '" + type.toString() + "'.");
          }
        }
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of schema specified is invalid. Please check the format.");
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    parseConfiguration(config.compressor);
    // Check if schema specified is a valid schema or no. 
    try {
      Schema outputSchema = Schema.parseJson(config.schema);
      List<Field> outFields = outputSchema.getFields();
      for(Field field : outFields) {
        outSchemaMap.put(field.getName(), field.getSchema().getType());
      }
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
    // in the fields that need to be encoded, if it's not then write 
    // to output as it is. 
    for(Field field : inFields) {
      LOG.info("Looking at field '" + field.getName() + "'");
      String name = field.getName();
      
      // Check if output schema also have the same field name. If it's not 
      // then throw an exception. 
      if(!outSchemaMap.containsKey(name)) {
        throw new Exception("Field " + name + " is not defined in the output.");  
      }
      
      Schema.Type outFieldType = outSchemaMap.get(name);
      
      // Check if the input field name is configured to be encoded. If the field is not 
      // present or is defined as none, then pass through the field as is. 
      if(!compMap.containsKey(name) || compMap.get(name) == CompDecompType.NONE) {
        builder.set(name, in.get(name));          
      } else {
        // Now, the input field could be of type String or byte[], so transform everything
        // to byte[] 
        byte[] obj = new byte[0];
        if (field.getSchema().getType() == Schema.Type.BYTES){
          obj = in.get(name);
        } else if (field.getSchema().getType() == Schema.Type.STRING) {
          obj = ((String)in.get(name)).getBytes();
        }
        
        // Now, based on the encode type configured for the field - encode the byte[] of the 
        // value.
        byte[] outValue = new byte[0];
        CompDecompType type = compMap.get(name);
        if(type == CompDecompType.SNAPPY) {
          outValue = Snappy.compress(obj);
        } else if (type == CompDecompType.ZIP) {
          outValue = compressZIP(obj);
        } else if (type == CompDecompType.GZIP) {
          outValue = compressGZIP(obj);
        }
        
        // Depending on the output field type, either convert it to 
        // Bytes or to String. 
        if(outFieldType == Schema.Type.BYTES) {
          builder.set(name, outValue);
        }
      }
    }
    emitter.emit(builder.build());
  }


  public static byte[] compressGZIP(byte[] input) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GZIPOutputStream gzip = new GZIPOutputStream(out);
    gzip.write(input, 0, input.length);
    gzip.close();
    return out.toByteArray();
  }

  public static byte[] compressZIP(byte[] input) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ZipOutputStream zip = new ZipOutputStream(out);
    zip.write(input,0, input.length);
    zip.close();
    return out.toByteArray();
  }

  
  public static class Config extends PluginConfig {
    @Name("compressor")
    @Description("Specify the field and compression type combination. " +
      "Format is <field>:<compressor-type>[,<field>:<compressor-type>]*")
    private final String compressor;

    @Name("schema")
    @Description("Specifies the output schema")
    private final String schema;
    
    public Config(String compressor, String schema) {
      this.compressor = compressor;
      this.schema = schema;
    }
  }
}

