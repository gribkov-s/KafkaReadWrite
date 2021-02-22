
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}


import java.util.Properties
import java.time.LocalDateTime
import scala.util.Try

object Producer extends App {

  implicit val formats = DefaultFormats

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val source = io.Source.fromFile("src/main/resources/bestsellers with categories.csv")
  val lines = source.getLines.drop(1)

  for (line <- lines) {

    val cols = line.split(",").map(_.trim)
    val book = Try {Book(cols(0), cols(1), cols(2).toDouble, cols(3).toInt, cols(4).toInt, cols(5).toInt, cols(6))}

    if (book.isSuccess) {
      val key = LocalDateTime.now().toString
      val value = write(book.get).toString
      val record = new ProducerRecord[String, String]("books", key, value)
      producer.send(record)
    }
  }

  producer.close()
  source.close
}
