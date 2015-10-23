package co.cask.hydrator.sinks;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.format.UnexpectedFormatException;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.realtime.DataWriter;
import co.cask.cdap.etl.api.realtime.RealtimeContext;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.spi.Registration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Kite Dataset Writer - Writes Kite Dataset in realtime. 
 */
@Plugin(type = "realtimesink")
@Name("KiteDatasetWriter")
@Description("Writes to Kite Dataset")
public class KiteDatasetWriter extends RealtimeSink<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(KiteDatasetWriter.class);
  
  private Config config;
  private DatasetWriter<GenericRecord> writer = null;
  private Dataset<GenericRecord> dataset;
  private boolean writerInitialized = false;
  
  // Only used while testing. 
  public KiteDatasetWriter(Config config) {
    this.config = config;
  } 
  
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
  }

  @Override
  public void initialize(RealtimeContext context) throws Exception {
    super.initialize(context);
  }

  @Override
  public int write(Iterable<StructuredRecord> records, DataWriter dataWriter) throws Exception {
    int count = 0;
    // Iterate through structured record.
    for (StructuredRecord record : records) {
      // Translate it to GenericRecord.
      GenericRecord genericRecord = transform(record);
      
      // This could be in initialize if there is a way to get input and output schema
      if (!writerInitialized) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
          Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
          if (!Datasets.exists(config.uri)) {
            DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
              .schema(genericRecord.getSchema()).build();
            dataset = Datasets.create(config.uri, descriptor);
          }
          writer = dataset.newWriter();
        } finally {
          Thread.currentThread().setContextClassLoader(cl);
          writerInitialized = true;
        }
      }
      
      // Writes the record to kite dataset.
      if(writer != null) {
        writer.write(genericRecord);
        count++;
      }
      
    }
    
    return count;
  }

  @Override
  public void destroy() {
    super.destroy();
    if(writer != null) {
      writer.close();
    }
  }

  private GenericRecord transform(StructuredRecord structuredRecord) throws IOException {
    co.cask.cdap.api.data.schema.Schema structuredRecordSchema = structuredRecord.getSchema();
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(structuredRecordSchema.toString());

    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(avroSchema);
    for ( org.apache.avro.Schema.Field field : avroSchema.getFields()) {
      String fieldName = field.name();
      recordBuilder.set(fieldName, convertField(structuredRecord.get(fieldName), field.schema()));
    }
    return recordBuilder.build();
  }

  private Object convertUnion(Object value, List<Schema> schemas) {
    boolean isNullable = false;
    for (org.apache.avro.Schema possibleSchema : schemas) {
      if (possibleSchema.getType() == org.apache.avro.Schema.Type.NULL) {
        isNullable = true;
        if (value == null) {
          return value;
        }
      } else {
        try {
          return convertField(value, possibleSchema);
        } catch (Exception e) {
          // if we couldn't convert, move to the next possibility
        }
      }
    }
    if (isNullable) {
      return null;
    }
    throw new UnexpectedFormatException("unable to determine union type.");
  }

  private List<Object> convertArray(Object values, org.apache.avro.Schema elementSchema) throws IOException {
    List<Object> output;
    if (values instanceof List) {
      List<Object> valuesList = (List<Object>) values;
      output = new ArrayList<Object>(valuesList.size());
      for (Object value : valuesList) {
        output.add(convertField(value, elementSchema));
      }
    } else {
      Object[] valuesArr = (Object[]) values;
      output = new ArrayList<Object>(valuesArr.length);
      for (Object value : valuesArr) {
        output.add(convertField(value, elementSchema));
      }
    }
    return output;
  }

  private Map<String, Object> convertMap(Map<String, Object> map, org.apache.avro.Schema valueSchema) throws IOException {
    Map<String, Object> converted = Maps.newHashMap();
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      converted.put(entry.getKey(), convertField(entry.getValue(), valueSchema));
    }
    return converted;
  }

  private Object convertField(Object field, org.apache.avro.Schema fieldSchema) throws IOException {
    org.apache.avro.Schema.Type fieldType = fieldSchema.getType();
    switch (fieldType) {
      case RECORD:
        return transform((StructuredRecord) field);
      case ARRAY:
        return convertArray(field, fieldSchema.getElementType());
      case MAP:
        return convertMap((Map<String, Object>) field, fieldSchema.getValueType());
      case UNION:
        return convertUnion(field, fieldSchema.getTypes());
      case NULL:
        return null;
      case STRING:
        return field.toString();
      case BYTES:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
        return field;
      default:
        throw new UnexpectedFormatException("field type " + fieldType + " is not supported.");
    }
  }

  public static class Config extends PluginConfig {
    @Name("uri")
    @Description("Specifies Kite Dataset URI")
    private String uri;

    public Config(String uri) {
      this.uri = uri;
    }
  }
}
