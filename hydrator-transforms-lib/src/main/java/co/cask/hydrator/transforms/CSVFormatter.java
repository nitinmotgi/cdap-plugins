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
import co.cask.cdap.etl.common.StructuredRecordStringConverter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

/**
 * A Transformation that parses a text into CSV Fields.
 */
@Plugin(type = "transform")
@Name("CSVFormatter")
@Description("Formats a Structure Record into CSV")
public final class CSVFormatter extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(CSVFormatter.class);
  private final Config config;

  // Output Schema associated with transform output.
  private Schema outSchema;

  // List of fields specified in the schema.
  private List<Field> fields;

  // Delimiter
  private char delim = ',';
  
  // Mapping from field, char
  private static final Map<String, String> delimiterMaps = Maps.newHashMap();
  
  // This is used only for tests, otherwise this is being injected by the ingestion framework.
  public CSVFormatter(Config config) {
    this.config = config;
  }

  static {
    delimiterMaps.put("COMMA", ",");
    delimiterMaps.put("CTRL-A", "\001");
    delimiterMaps.put("TAB", "\t");
    delimiterMaps.put("VBAR", "|");
    delimiterMaps.put("STAR", "*");
    delimiterMaps.put("CARROT", "^");
    delimiterMaps.put("DOLLAR", "$");
    delimiterMaps.put("HASH", "#");
    delimiterMaps.put("TILDE", "~");
    delimiterMaps.put("CTRL-B", "\002");
    delimiterMaps.put("CTRL-C", "\003");
    delimiterMaps.put("CTRL-D", "\004");
    delimiterMaps.put("CTRL-E", "\005");
    delimiterMaps.put("CTRL-F", "\006");
  }
  
  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);

    try {
      outSchema = Schema.parseJson(config.schema);
      fields = outSchema.getFields();
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of schema specified is invalid. Please check the format.");
    }
    
    if(delimiterMaps.containsKey(config.delimiter)) {
      delim = delimiterMaps.get(config.delimiter).charAt(0);
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    
    if (!delimiterMaps.containsKey(config.delimiter)) {
      throw new IllegalArgumentException("Unknown delimiter '" + config.delimiter + "' specified. ");
    }
    
    // Check if the format specified is valid.
    if(config.format == null || config.format.isEmpty()) {
      throw new IllegalArgumentException("Format is not specified. Allowed values are DEFAULT, EXCEL, MYSQL," +
                                           " RFC4180 & TDF");
    }
    
    if(!config.format.equalsIgnoreCase("DELIMITER") && !config.format.equalsIgnoreCase("EXCEL") &&
        !config.format.equalsIgnoreCase("MYSQL") && !config.format.equalsIgnoreCase("RFC4180") &&
        !config.format.equalsIgnoreCase("TDF")) {
      throw new IllegalArgumentException("Format specified is not one of the allowed values. Allowed values are " +
                                           "DELIMITER, EXCEL, MYSQL, RFC4180 & TDF");
    }
    
    // Check if schema specified is a valid schema or no. 
    try {
      Schema schema = Schema.parseJson(config.schema);
      List<Schema.Field> fields = schema.getFields();
      if (fields.size() > 1) {
        throw new IllegalArgumentException("Output schema should have only one field of type String");
      }
      if(fields.get(0).getSchema().getType() != Schema.Type.STRING) {
        throw new IllegalArgumentException("Output field type should be String");  
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of schema specified is invalid. Please check the format.");
    }
    
  }

  @Override
  public void transform(StructuredRecord record, Emitter<StructuredRecord> emitter) throws Exception {
    String body = "";
    if (config.format.equalsIgnoreCase("JSON")) {
      body = StructuredRecordStringConverter.toJsonString(record);
    } else {
      // Extract all values from the structured record
      List<Object> objs = Lists.newArrayList();
      for(Schema.Field field : fields) {
        objs.add(record.get(field.getName()));
      }

      StringWriter writer = new StringWriter();
      CSVPrinter printer = null;
      CSVFormat csvFileFormat = null;
      switch(config.format.toLowerCase()) {
        case "delimiter":
          LOG.info("Delimiter type " + delim);
          csvFileFormat = CSVFormat.newFormat(delim).withQuote('"')
            .withRecordSeparator("\r\n").withIgnoreEmptyLines();
          printer = new CSVPrinter(writer, csvFileFormat);
          break;

        case "excel":
          csvFileFormat = CSVFormat.Predefined.Excel.getFormat();
          printer = new CSVPrinter(writer, csvFileFormat);
          break;

        case "mysql":
          csvFileFormat = CSVFormat.Predefined.MySQL.getFormat();
          printer = new CSVPrinter(writer, csvFileFormat);
          break;

        case "tdf":
          csvFileFormat = CSVFormat.Predefined.TDF.getFormat();
          printer = new CSVPrinter(writer, csvFileFormat);
          break;

        case "rfc4180":
          csvFileFormat = CSVFormat.Predefined.TDF.getFormat();
          printer = new CSVPrinter(writer, csvFileFormat);
          break;
      }
      
      if (printer != null) {
        printer.printRecord(objs);
        body = writer.toString();
      }
    }

    emitter.emit(StructuredRecord.builder(outSchema)
      .set(outSchema.getFields().get(0).getName(), body)
      .build());
  }



  /**
   * Configuration for the plugin.
   */
  public static class Config extends PluginConfig {
    
    @Name("format")
    @Description("Specify one of the predefined formats. DEFAULT, EXCEL, MYSQL, RFC4180 & TDF are supported formats.")
    private final String format;
    
    @Name("delimiter")
    @Description("Specify delimiter to be used for separating fields.")
    private final String delimiter;
    
    @Name("schema")
    @Description("Specifies the schema that has to be output.")
    private final String schema;
    
    public Config(String format, String delimiter, String schema) {
      this.format = format;
      this.delimiter = delimiter;
      this.schema = schema;
    }
  }
  
  
}

