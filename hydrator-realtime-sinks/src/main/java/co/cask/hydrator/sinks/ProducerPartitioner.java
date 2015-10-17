package co.cask.hydrator.sinks;

import kafka.producer.Partitioner;


public class ProducerPartitioner implements Partitioner {
  @Override
  public int partition(Object key, int numPartitions) {
    int partition = 0;
    Integer partitionId = (Integer) key;
    return partitionId % numPartitions;
  }
}
