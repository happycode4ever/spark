package com.jj.spark.streaming.input.kafka.direct

import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.JavaConversions._

//包装kafka客户端
class KafkaProxy(brokers: String) {
  val kafkaProp = Map(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop112:9092,hadoop113:9092,hadoop114:9092",
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getName,
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getName
  )

  val kafkaClient = new KafkaProducer[String, String](kafkaProp)
}
//创建一个创建KafkaProxy的工厂
class KafkaProxyFactory(brokers: String) extends BasePooledObjectFactory[KafkaProxy] {
  //创建实例
  override def create(): KafkaProxy = new KafkaProxy(brokers)
  //包装实例
  override def wrap(t: KafkaProxy): PooledObject[KafkaProxy] = new DefaultPooledObject[KafkaProxy](t)
}

//单例连接池
object KafkaPool {
  private var kafkaPool: GenericObjectPool[KafkaProxy] = null

  def apply(brokers: String): GenericObjectPool[KafkaProxy] = {
    if(kafkaPool==null){
      //运用字节码对象的线程锁实现懒汉式单例，线程安全
      KafkaPool.synchronized{
        //通过工厂对象获取
        this.kafkaPool = new GenericObjectPool[KafkaProxy](new KafkaProxyFactory(brokers))
      }
    }
    kafkaPool
  }
}
