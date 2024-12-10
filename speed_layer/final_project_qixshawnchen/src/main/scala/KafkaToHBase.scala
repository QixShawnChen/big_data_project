import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

// Case class for parsed data
case class CountryData(
  composite_key: String,
  country_id: String,
  country_name: String,
  year: Int,
  NGDP_RPCH: Option[Float] = None,
  PCPIPCH: Option[Float] = None,
  PPPPC: Option[Float] = None,
  PPPGDP: Option[Float] = None,
  LP: Option[Float] = None,
  BCA: Option[Float] = None,
  LUR: Option[Float] = None,
  rev: Option[Float] = None,
  GGXCNL_NGDP: Option[Float] = None,
  NGS_GDP: Option[Float] = None,
  GGXCNL_GDP: Option[Float] = None,
  NI_GDP: Option[Float] = None
)

object KafkaToHBase {
  val logger = LoggerFactory.getLogger(this.getClass)
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  // HBase connection setup
  val hbaseConf = HBaseConfiguration.create()
  val hbaseConnection: Connection = ConnectionFactory.createConnection(hbaseConf)
  val formalDataTable = hbaseConnection.getTable(TableName.valueOf("qixshawnchen_final_project_speed_hbase"))

  // Parse a Kafka message into a list of CountryData objects
  def parseKafkaMessage(message: String): List[CountryData] = {
    try {
      val rootNode = mapper.readTree(message)
      val dataNode = rootNode.get("data")

      if (dataNode != null) {
        val metrics = dataNode.fields().asScala
        metrics.flatMap { metricEntry =>
          val metricName = metricEntry.getKey
          val countries = metricEntry.getValue.fields().asScala

          countries.map { countryEntry =>
            val countryCode = countryEntry.getKey
            val countryNode = countryEntry.getValue
            val compositeKey = s"${countryCode}_${countryNode.get("year").asText()}"

            CountryData(
              composite_key = compositeKey,
              country_id = countryCode,
              country_name = countryNode.get("country_name").asText(),
              year = countryNode.get("year").asInt(),
              NGDP_RPCH = if (metricName == "NGDP_RPCH") Some(countryNode.get("value").floatValue()) else None,
              PCPIPCH = if (metricName == "PCPIPCH") Some(countryNode.get("value").floatValue()) else None,
              PPPPC = if (metricName == "PPPPC") Some(countryNode.get("value").floatValue()) else None,
              PPPGDP = if (metricName == "PPPGDP") Some(countryNode.get("value").floatValue()) else None,
              LP = if (metricName == "LP") Some(countryNode.get("value").floatValue()) else None,
              BCA = if (metricName == "BCA") Some(countryNode.get("value").floatValue()) else None,
              LUR = if (metricName == "LUR") Some(countryNode.get("value").floatValue()) else None,
              rev = if (metricName == "rev") Some(countryNode.get("value").floatValue()) else None,
              GGXCNL_NGDP = if (metricName == "GGXCNL_NGDP") Some(countryNode.get("value").floatValue()) else None,
              NGS_GDP = if (metricName == "NGS_GDP") Some(countryNode.get("value").floatValue()) else None,
              GGXCNL_GDP = if (metricName == "GGXCNL_GDP") Some(countryNode.get("value").floatValue()) else None,
              NI_GDP = if (metricName == "NI_GDP") Some(countryNode.get("value").floatValue()) else None
            )
          }
        }.toList
      } else {
        logger.warn("No 'data' field found in message.")
        List.empty[CountryData]
      }
    } catch {
      case e: Exception =>
        logger.error("Error parsing message: " + e.getMessage, e)
        List.empty[CountryData]
    }
  }

  // Write data to HBase
  def writeToHBase(record: CountryData): Unit = {
    try {
      val put = new Put(Bytes.toBytes(record.composite_key))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("country_id"), Bytes.toBytes(record.country_id))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("country_name"), Bytes.toBytes(record.country_name))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("year"), Bytes.toBytes(record.year))

      // Add each metric as a string for readability
      record.NGDP_RPCH.foreach(value => put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("NGDP_RPCH"), Bytes.toBytes(value.toString)))
      record.PCPIPCH.foreach(value => put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("PCPIPCH"), Bytes.toBytes(value.toString)))
      record.PPPPC.foreach(value => put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("PPPPC"), Bytes.toBytes(value.toString)))
      record.PPPGDP.foreach(value => put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("PPPGDP"), Bytes.toBytes(value.toString)))
      record.LP.foreach(value => put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("LP"), Bytes.toBytes(value.toString)))
      record.BCA.foreach(value => put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("BCA"), Bytes.toBytes(value.toString)))
      record.LUR.foreach(value => put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("LUR"), Bytes.toBytes(value.toString)))
      record.rev.foreach(value => put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rev"), Bytes.toBytes(value.toString)))
      record.GGXCNL_NGDP.foreach(value => put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("GGXCNL_NGDP"), Bytes.toBytes(value.toString)))
      record.NGS_GDP.foreach(value => put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("NGS_GDP"), Bytes.toBytes(value.toString)))
      record.GGXCNL_GDP.foreach(value => put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("GGXCNL_GDP"), Bytes.toBytes(value.toString)))
      record.NI_GDP.foreach(value => put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("NI_GDP"), Bytes.toBytes(value.toString)))

      formalDataTable.put(put)
    } catch {
      case e: Exception => logger.error("Error writing to HBase: " + e.getMessage, e)
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      logger.error("Usage: KafkaToHBase <brokers>")
      System.exit(1)
    }

    val Array(brokers) = args
    val sparkConf = new SparkConf().setAppName("KafkaToHBase")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "latest_message_consumer",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("qixshawnchen_econ_data_final")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics.toSet, kafkaParams)
    )

    sys.addShutdownHook {
      logger.info("Shutting down application...")
      if (!hbaseConnection.isClosed) hbaseConnection.close()
    }

    stream.foreachRDD { rdd =>
      rdd.foreach { record =>
        val parsedRecords = parseKafkaMessage(record.value())
        parsedRecords.foreach(writeToHBase)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}



