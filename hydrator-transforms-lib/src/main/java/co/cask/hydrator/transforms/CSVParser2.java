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
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.List;

/**
 * A Transformation that parses a text into CSV Fields.
 */
@Plugin(type = "transform")
@Name("CSVParser2")
@Description("Decodes, Decompresses and Parses CSV Records.")
public class CSVParser2 extends Transform<StructuredRecord, StructuredRecord> {
  private final Config config;

  // Output Schema associated with transform output.
  private Schema outSchema;

  // List of fields specified in the schema.
  private List<Field> fields;

  // Format of CSV.
  private CSVFormat csvFormat = CSVFormat.DEFAULT;

  // This is used only for tests, otherwise this is being injected by the ingestion framework.
  public CSVParser2(Config config) {
    this.config = config;
  }
  
  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    
    String csvFormatString = config.format.toLowerCase();
    switch(csvFormatString) {
      case "default":
        csvFormat = CSVFormat.DEFAULT;
        break;
      
      case "excel":
        csvFormat = CSVFormat.EXCEL;
        break;
      
      case "mysql":
        csvFormat = CSVFormat.MYSQL;
        break;
      
      case "rfc4180":
        csvFormat = CSVFormat.RFC4180;
        break;
      
      case "tdf":
        csvFormat = CSVFormat.TDF;
        break;
      
      default:
        throw new IllegalArgumentException("Format {} specified is not one of the allowed format. Allowed formats are" +
                                             "DEFAULT, EXCEL, MYSQL, RFC4180 and TDF");
    }
    
    if(config.field == null || config.field.isEmpty()) {
      throw new IllegalArgumentException("Field for applying transformation is not specified.");
    }

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
    
    // Check if the format specified is valid.
    if(config.format == null || config.format.isEmpty()) {
      throw new IllegalArgumentException("Format is not specified. Allowed values are DEFAULT, EXCEL, MYSQL," +
                                           " RFC4180 & TDF");
    }
    
    if(!config.format.equalsIgnoreCase("DEFAULT") && !config.format.equalsIgnoreCase("EXCEL") &&
        !config.format.equalsIgnoreCase("MYSQL") && !config.format.equalsIgnoreCase("RFC4180") &&
        !config.format.equalsIgnoreCase("TDF")) {
      throw new IllegalArgumentException("Format specified is not one of the allowed values. Allowed values are " +
                                           "DEFAULT, EXCEL, MYSQL, RFC4180 & TDF");
    }
    
    // Check if the decoder specified is one of the allowed types.
    if(!config.decompress.equalsIgnoreCase("BASE64") && !config.decompress.equalsIgnoreCase("BASE32") 
      && !config.decompress.equalsIgnoreCase("NONE") && !config.decompress.equalsIgnoreCase("HEX")) {
      throw new IllegalArgumentException("Unsupported decoder '" + config.decoder + ", specified. Supported types are" +
                                           "NONE, BASE64, BASE32 and HEX");
    }
    
    // Check if the decompressor specified is one of the allowed types.
    if(!config.decompress.equalsIgnoreCase("SNAPPY") && !config.decompress.equalsIgnoreCase("NONE")) {
      throw new IllegalArgumentException("Unsupported decompressor algorithm '" + config.decompress + 
                                           "' specified. Currently supports NONE & SNAPPY");
    }
    
    // Check if schema specified is a valid schema or no. 
    try {
      Schema.parseJson(config.schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of schema specified is invalid. Please check the format.");
    }
    
  }

  @Override
  public void transform(StructuredRecord in, Emitter<StructuredRecord> emitter) throws Exception {
    // Field has to string to be parsed correctly. For others throw an exception.
    String body = in.get(config.field);
    
    // If decoder is not NONE, then apply decoder.
    byte[] decodedPayLoad;
    if(!config.decoder.equalsIgnoreCase("NONE")) {
      decodedPayLoad = decodePayLoad(body);   
    } else {
      decodedPayLoad = body.getBytes();
    }
    
    // If decompess is not NONE, then apply decompressor.
    byte[] uncompressedPayLoad = decodedPayLoad.clone();
    if(!config.decompress.equalsIgnoreCase("NONE")) {
     if(config.decompress.equalsIgnoreCase("SNAPPY")) {
       uncompressedPayLoad = Snappy.uncompress(decodedPayLoad);
     }
    }
    
    // Parse the text as CSV and emit it as structured record.
    try {
      CSVParser parser = CSVParser.parse(new String(uncompressedPayLoad), csvFormat);
      List<CSVRecord> records = parser.getRecords();
      for(CSVRecord record : records ) {
        if(fields.size() == record.size()) {
          StructuredRecord sRecord = createStructuredRecord(record);
          emitter.emit(sRecord);
        } else {
          // Write the record to error Dataset.
        }
      }
    } catch (IOException e) {

    }
  }

  /**
   * Decodes the payload. 
   *  
   * @param body
   * @return
   */
  private byte[] decodePayLoad(String body) throws DecoderException {
    if(config.decoder.equalsIgnoreCase("base64")) {
      Base64 codec = new Base64();
      return codec.decode(body);
    } else if (config.decoder.equalsIgnoreCase("base32")){
      Base32 codec = new Base32();
      return codec.decode(body);
    } else if (config.decoder.equalsIgnoreCase("hex")) {
      Hex codec = new Hex();
      return codec.decode(body.getBytes());
    }
    return new byte[0];
  }

  private StructuredRecord createStructuredRecord(CSVRecord record) {
    StructuredRecord.Builder builder = StructuredRecord.builder(outSchema);
    int i = 0;
    for(Field field : fields) {
      builder.set(field.getName(), TypeConvertors.get(record.get(i), field.getSchema().getType()));
      ++i;
    }
    return builder.build();
  }


  /**
   * Configuration for the plugin.
   */
  public static class Config extends PluginConfig {
    
    @Name("decode")
    @Description("Specify the decoder to be applied on the payload.")
    private final String decoder;
    
    @Name("decompress")
    @Description("Specifies decompress algorithm to be applied to decoded payload.")
    private final String decompress;
    
    @Name("format")
    @Description("Specify one of the predefined formats. DEFAULT, EXCEL, MYSQL, RFC4180 & TDF are supported formats.")
    private final String format;
    
    @Name("field")
    @Description("Specify the field that should be used for parsing into CSV.")
    private final String field;
    
    @Name("schema")
    @Description("Specifies the schema that has to be output.")
    private final String schema;
    
    public Config(String decoder, String decompress, String format, String field, String schema) {
      this.decoder = decoder;
      this.decompress = decompress;
      this.format = format;
      this.field = field;
      this.schema = schema;
    }
  }
  
  
}

