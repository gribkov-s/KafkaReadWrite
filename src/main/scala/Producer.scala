
import java.nio.file.Files
import java.nio.file.Paths
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import scala.util.Try

import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import java.util.Properties
import java.time.LocalDateTime


object Producer extends App {

  implicit val formats = DefaultFormats

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val path = "src/main/resources/bestsellers with categories.csv"
  val reader = Files.newBufferedReader(Paths.get(path))
  val csvParser = new CSVParser(reader, CSVFormat.RFC4180)
  val records = csvParser.getRecords

  records.forEach(rec => {
    val book = Try {
      Book(
        rec.get(0),
        rec.get(1),
        rec.get(2).toDouble,
        rec.get(3).toInt,
        rec.get(4).toInt,
        rec.get(5).toInt,
        rec.get(6)
      )
    }

    if (book.isSuccess) {
      val key = LocalDateTime.now().toString
      val value = write(book.get).toString
      val record = new ProducerRecord[String, String]("books", key, value)
      producer.send(record)
    }
  })

  producer.close()
}
