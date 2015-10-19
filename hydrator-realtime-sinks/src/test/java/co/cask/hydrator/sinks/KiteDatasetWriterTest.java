package co.cask.hydrator.sinks;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.format.UnexpectedFormatException;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Test;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Datasets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by nmotgi on 10/17/15.
 */
public class KiteDatasetWriterTest {
  private static final Logger LOG = LoggerFactory.getLogger(KiteDatasetWriter.class);

  // Input Schema that would be injested into Kafka.
  private static final Schema INPUT = Schema.recordOf("input",
                                                      Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                                      Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("c", Schema.of(Schema.Type.INT)),
                                                      Schema.Field.of("d", Schema.of(Schema.Type.DOUBLE)),
                                                      Schema.Field.of("e", Schema.of(Schema.Type.BOOLEAN)));

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

  private Object convertUnion(Object value, List<org.apache.avro.Schema> schemas) {
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

  protected Object convertField(Object field, org.apache.avro.Schema fieldSchema) throws IOException {
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
  
  @Test
  public void testKiteSDKRaw() throws Exception {
    String datasetUri = "dataset:file:/tmp/hellos";
    StructuredRecord record = 
      StructuredRecord.builder(INPUT).set("a", 1L).set("b", "first")
        .set("c", 1).set("d", 12.34).set("e", false).build();
    GenericRecord genericRecord = transform(record);
    
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
      .schema(genericRecord.getSchema()).build();
    Datasets.delete(datasetUri);
    Dataset<GenericRecord> hellos = Datasets.create(datasetUri, descriptor);


    // Write some Hellos in to the dataset
    DatasetWriter<GenericRecord> writer = null;
    try {
      writer = hellos.newWriter();
      writer.write(genericRecord);
    } finally {
      if (writer != null) {
        writer.close();
      }
    }

    DatasetReader<GenericRecord> reader = null;
    try {
      reader = hellos.newReader();

      for (GenericRecord hello : reader) {
        LOG.info(hello.toString());
      }

    } finally {
      if (reader != null) {
        reader.close();
      }
    }    
    
  }

  @Test
  public void testKiteSDKPlugin() throws Exception {
    String uri = "dataset:file:/tmp/testKiteSDKPlugin";
    Datasets.delete(uri);
    KiteDatasetWriter.Config config = new KiteDatasetWriter.Config(uri);
    RealtimeSink<StructuredRecord> kitewriter = new KiteDatasetWriter(config);
    kitewriter.initialize(new MockRealtimeContext());
    List<StructuredRecord> input = Lists.newArrayList();
    input.add(StructuredRecord.builder(INPUT).set("a", 1L).set("b", "first").set("c", 1).set("d", 12.34)
                .set("e", false).build());
    input.add(StructuredRecord.builder(INPUT).set("a", 2L).set("b", "second").set("c", 2).set("d", 13.34)
                .set("e", true).build());
    input.add(StructuredRecord.builder(INPUT).set("a", 3L).set("b", "third").set("c", 3).set("d", 14.34)
                .set("e", false).build());
    input.add(StructuredRecord.builder(INPUT).set("a", 4L).set("b", "fourth").set("c", 4).set("d", 15.34)
                .set("e", true).build());
    kitewriter.write(input, null);
    kitewriter.destroy();
  }

}