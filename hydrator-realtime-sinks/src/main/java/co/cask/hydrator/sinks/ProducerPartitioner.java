package co.cask.hydrator.sinks;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;


public class ProducerPartitioner implements Partitioner {

  public ProducerPartitioner (VerifiableProperties props) {}
  
  @Override
  public int partition(Object key, int numPartitions) {
    int partition = 0;
    Integer partitionId = (Integer) key;
    return partitionId % numPartitions;
  }
}
