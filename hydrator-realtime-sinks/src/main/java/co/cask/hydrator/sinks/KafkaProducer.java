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

import java.util.Collections;
import java.util.Comparator;
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
  private final SinkConfig sconfig;
  private Properties props;
  private ProducerConfig config;
  private Producer<String, String> producer;
  private RealtimeContext context;
  private boolean fieldsExtracted = false;
  private String[] topics;
  
  
  public KafkaProducer(SinkConfig config) {
    this.sconfig = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
  }

  @Override
  public void initialize(RealtimeContext context) throws Exception {
    super.initialize(context);
    this.context = context;
    topics = sconfig.topics.split(",");
    props.put("metadata.broker.list", sconfig.brokerList);
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("partitioner.class", "co.cask.hydrator.sinks.ProducerPartitioner");
    if (sconfig.requireAck.equalsIgnoreCase("TRUE")) {
      props.put("request.required.acks", "1");
    }
    config = new ProducerConfig(props);
    producer = new Producer<String, String>(config);
    
  }
  
  @Override
  public int write(Iterable<StructuredRecord> objects, DataWriter dataWriter) throws Exception { 
    int count = 0;

    List<Schema.Field> fields = Lists.newArrayList();
    for (StructuredRecord object : objects) {
      if(!fieldsExtracted) {
        Collections.sort(fields, new Comparator<Schema.Field>() {
          @Override
          public int compare(Schema.Field o1, Schema.Field o2) {
            return o1.getName().compareTo(o2.toString());
          }
        });
      }
      
      String body = "";
      if (sconfig.format.equalsIgnoreCase("JSON")) {
        body = StructuredRecordStringConverter.toJsonString(object);
      }
      
      if (sconfig.format.equalsIgnoreCase("CSV") || sconfig.format.equalsIgnoreCase("EXCEL")
          || sconfig.format.equalsIgnoreCase("MYSQL") || sconfig.format.equalsIgnoreCase("TDF") 
          || sconfig.format.equalsIgnoreCase("RFC4180")) {
        List<Object> objs = Lists.newArrayList();
        for(Schema.Field field : fields) {
          objs.add(object.get(field.getName()));
        }
        
        if(sconfig.format.equalsIgnoreCase("CSV")) {
          body = CSVFormat.DEFAULT.format(objs);
        } else if (sconfig.format.equalsIgnoreCase("EXCEL")) {
          body = CSVFormat.EXCEL.format(objs);
        } else if (sconfig.format.equalsIgnoreCase("MYSQL")) {
          body = CSVFormat.MYSQL.format(objs);
        } else if (sconfig.format.equalsIgnoreCase("TDF")) {
          body = CSVFormat.TDF.format(objs);
        } else if (sconfig.format.equalsIgnoreCase("RFC4180")) {
          body = CSVFormat.RFC4180.format(objs);
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
    
    @Name("brokerlist")
    @Description("Specifies the connection string where Producer can find one or more brokers to " +
      "determine the leader for each topic")
    private String brokerList;
    
    @Name("requireacks")
    @Description("Tells Kafka that you require an acknowledgement from Broker that message was received")
    private String requireAck;
    
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
  }
}


