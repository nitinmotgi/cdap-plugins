package co.cask.hydrator.sinks;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinPartitioner implements Partitioner {
  private static final Logger log = LoggerFactory.getLogger(RoundRobinPartitioner.class);

  final AtomicInteger counter = new AtomicInteger(0);

  public RoundRobinPartitioner (VerifiableProperties props) {}

  /**
   * Take key as value and return the partition number
   */
  public int partition(Object key, int partitions) {
    int partitionId = counter.incrementAndGet() % partitions;
    if (counter.get() > 65536) {
      counter.set(0);
    }
    return partitionId;
  }
}
