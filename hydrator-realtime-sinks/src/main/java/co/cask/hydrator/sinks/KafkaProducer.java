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

package co.cask.hydrator.sinks;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.realtime.DataWriter;
import co.cask.cdap.etl.api.realtime.RealtimeContext;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import co.cask.cdap.etl.common.StructuredRecordStringConverter;
import com.google.common.collect.Lists;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.csv.CSVFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;


/**
 * Real-time Kafka sink.
 */
@Plugin(type = "realtimesink")
@Name("KafkaProducer")
@Description("Real-time Kafka producer")
public class KafkaProducer extends RealtimeSink<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaProducer.class);
  private static final String BROKER_LIST = "metadata.broker.list";
  private static final String SERIALIZER = "serializer.class";
  private static final String PARTITIONER = "partitioner.class";
  private static final String ACKS_REQUIRED = "request.required.acks";
  
  private final SinkConfig sconfig;
  
  // Kafka properties
  private final Properties props = new Properties();
  
  // Kafka procduer configuration
  private ProducerConfig config;
  
  // Kafka producer handle
  private Producer<String, String> producer;
  
  // Plugin context
  private RealtimeContext context;
  
  // Optimization to collect fields extracted. This is required because the schema
  // is not available during initialization and configuration phase.
  private boolean fieldsExtracted = false;
  
  // List of Kafka topics.
  private String[] topics;
  
  
  // required for testing.
  public KafkaProducer(SinkConfig config) {
    this.sconfig = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    
    // Validations to be added.
  }

  @Override
  public void initialize(RealtimeContext context) throws Exception {
    super.initialize(context);
    this.context = context;

    // Extract the topics
    topics = sconfig.topics.split(",");
    
    // Configure the properties for kafka.
    props.put(BROKER_LIST, sconfig.brokers);
    props.put(SERIALIZER, "kafka.serializer.StringEncoder");
    props.put(PARTITIONER, "co.cask.hydrator.sinks.ProducerPartitioner");
    if (sconfig.ackRequired.equalsIgnoreCase("TRUE")) {
      props.put(ACKS_REQUIRED, "1");
    }
    
    config = new ProducerConfig(props);
    producer = new Producer<String, String>(config);
  }
  
  @Override
  public int write(Iterable<StructuredRecord> objects, DataWriter dataWriter) throws Exception { 
    int count = 0;

    List<Schema.Field> fields = Lists.newArrayList();
    
    // For each object
    for (StructuredRecord object : objects) {
      
      // Extract the field names from the object passed in. This is required
      // because this information is not available in initialize or configuration phase.
      if(!fieldsExtracted) {
        fields = object.getSchema().getFields();
        fieldsExtracted = true;
      }
      
      // Depending on the configuration create a body that needs to be 
      // built and pushed to Kafka. 
      String body = "";
      if (sconfig.format.equalsIgnoreCase("JSON")) {
        body = StructuredRecordStringConverter.toJsonString(object);
      } else {
        // Extract all values from the structured record
        List<Object> objs = Lists.newArrayList();
        for(Schema.Field field : fields) {
          objs.add(object.get(field.getName()));
        }
        
        switch(sconfig.format.toLowerCase()) {
          case "csv":
            body = CSVFormat.DEFAULT.format(objs);
            break;
          
          case "excel":
            body = CSVFormat.EXCEL.format(objs);
            break;
          
          case "mysql":
            body = CSVFormat.MYSQL.format(objs);
            break;
          
          case "tdf":
            body = CSVFormat.TDF.format(objs);
            break;
          
          case "rfc4180":
            body = CSVFormat.RFC4180.format(objs);
            break;
        }
      }
      
      // Message key.
      String key = "no_key";
      if(sconfig.key != null) {
        key = object.get(sconfig.key);    
      }
      
      // Extract the partition key from the record
      Integer partitionKey = 0;
      if(sconfig.partitionField != null) {
        partitionKey = object.get(sconfig.partitionField);
      }

      // Write to all the configured topics
      for(String topic : topics) {
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, key, partitionKey, body);
        producer.send(data);
      }
      
    }
    context.getMetrics().count("kafka.producer.count", count);
    return count;
  }


  @Override
  public void destroy() {
    super.destroy();
    producer.close();
  }

  public static class SinkConfig extends PluginConfig {
    
    @Name("brokers")
    @Description("Specifies the connection string where Producer can find one or more brokers to " +
      "determine the leader for each topic")
    private String brokers;
    
    @Name("ackrequired")
    @Description("Specifies whether an acknowledgment is required from broker that message was received. " +
      "Default is FALSE")
    private String ackRequired;
    
    @Name("partitionfield")
    @Description("Specify field that should be used as partition ID. Should be a int or long")
    private String partitionField;

    @Name("key")
    @Description("Specify the key field to be used in the message")
    private String key;
    
    @Name("topics")
    @Description("List of topics to which message needs to be published")
    private String topics;
    
    @Name("format")
    @Description("Format a structured record should be converted to")
    private String format;
    
    
    public SinkConfig(String brokers, String ackRequired, String partitionField, String key, String topics,
                      String format) {
      this.brokers = brokers;
      this.ackRequired = ackRequired;
      this.partitionField = partitionField;
      this.key = key;
      this.topics = topics;
      this.format = format;
    }
  }
}


