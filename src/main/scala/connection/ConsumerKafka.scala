package connection

import java.time.Duration
import java.util
import java.util.{Collections, Map, Properties}

import kafka.common
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import mysqlutil.DBUtils

import scala.collection.mutable
import scala.collection.parallel.immutable
object ConsumerKafka {
  def main(args: Array[String]): Unit = {
    val groupId = "group_five"
    val topic = "CRV_ES"
    val properties = new Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"cdhcm.fahaicc.com:9092,cdh1.fahaicc.com:9092,cdh2.fahaicc.com:9092")
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
    properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId)
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest")
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false")

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String,String](properties)
    consumer.subscribe(Collections.singletonList(topic),new ConsumerRebalanceListener {

      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
        val it = partitions.iterator()
        while(it.hasNext){
          val partition: TopicPartition = it.next()
          val topic_save: String = partition.topic()
          val partition_save: Int = partition.partition()
          val offset: Long = consumer.position(partition)
          val timestamp:String = String.valueOf(System.currentTimeMillis())
          val sql : String = "replace into offset values(?,?,?,?,?)"
          DBUtils.update(sql,groupId,topic_save,partition_save.toString,offset.toString,timestamp)
        }
      }

      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
        val it: util.Iterator[TopicPartition] = partitions.iterator()
        while(it.hasNext){
          val partition: TopicPartition = it.next()
          val partition_s: Int = partition.partition()
          val topic_s: String = partition.topic()
          val sql : String  = "select sub_topic_partition_offset from offset where consumer_group= ? and sub_topic= ? and sub_topic_partition_id = ? "
          val offsets: Long = DBUtils.queryOnlyOffset(sql,groupId,topic_s,partition_s.toString)
          consumer.seek(partition,offsets)
        }
      }
    })
    while(true){
      val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(100))
      val it: util.Iterator[ConsumerRecord[String, String]] = records.iterator()
      val partitionToLong: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition,Long]()
      while(it.hasNext){
        val record: ConsumerRecord[String, String] = it.next()
        println(s"key:${record.key()},value:${record.value()}")
        val topic_partition = new TopicPartition(record.topic(),record.partition())
        partitionToLong+=(topic_partition ->record.offset())


      }
      val sql:String  = "replace into offset values(?,?,?,?,?)"
      for((k,v) <- partitionToLong){
        val topic_s: String = k.topic()
        val partition_s: Int = k.partition()
        DBUtils.update(sql,groupId,topic_s,partition_s.toString,v.toString,System.currentTimeMillis().toString)
      }



    }
  }
}

