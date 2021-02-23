
import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig}
import org.apache.kafka.common.TopicPartition
import scala.jdk.CollectionConverters._

import java.util.Properties
import java.time.Duration

object Consumer extends App {

  val partNum = 3
  val msgNum = 5
  val props = new Properties()

  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "books1")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

  val consumer = new KafkaConsumer(props)
  val partitions = consumer.partitionsFor("books")

  for(i <- 0 until partNum) {

    val partition = new TopicPartition("books", i)
    consumer.assign(List(partition).asJavaCollection)

    consumer.seekToEnd(List(partition).asJavaCollection)
    val endPosition = this.consumer.endOffsets(List(partition).asJavaCollection).get(partition)
    val actualPosition = consumer.position(partition) - msgNum
    consumer.seek(partition, actualPosition)

    val records = consumer
      .poll(Duration.ofSeconds(1))
      .asScala

    records.foreach(p => println(p.partition() + ", " + p.offset() + ", " + p.key(), ": ", p.value()))
  }

  consumer.close()
}