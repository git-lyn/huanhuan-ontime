package com.wmidop.datacollector.filter

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
//import com.wmidop.datacollector.bean.Vehicle
//import com.wmidop.datacollector.drools.service.DroolsRedisVehicle
import com.wmidop.datacollector.util.KafkaConnPool
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}

object VehicleFilterStreaming {
 /* val stream = VehicleFilterStreaming.getClass.getClassLoader.getResourceAsStream("kafka-config.properties")
  val prop = new Properties()
  prop.load(stream)
  //获取配置参数
  val brokenList = prop.getProperty("kafka.broker.list.vehicle")
  //  到目标集群的kakfa服务
  val targetTopic_brokenList = prop.getProperty("targetTopic.kafka.broker.list.vehicle")
  val zookeeper = prop.getProperty("kafka.zk.list.vehicle")
  val sourceTopic = prop.getProperty("kafka.source.topic.vehicle")
  val targetTopic = prop.getProperty("kafka.target.topic")
  val groupid = prop.getProperty("kafka.consumer.groupid")

  def main(args: Array[String]): Unit = {
    //创建sparkConf对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("FilterKafkaWithStreaming")
    //创建StreamingContext对象
    val ssc = new StreamingContext(sparkConf,Seconds(5));
    //  从kafka中获取对应的信息流数据
    val textKafkaDStream = getDStream(sparkConf,ssc)

    var offsetRanges = Array[OffsetRange]()
    //注意，要想获得offsetRanges必须作为第一步

    val textKafkaDStream2 = textKafkaDStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    //  进行数据的封装
    val vehicleDstream = textKafkaDStream2.map{rdd =>
      val key = rdd._1
      val value = rdd._2
      val vehicle = JSON.parseObject(value,classOf[Vehicle])
      (key,vehicle)
    }

    //  使用规则引擎drools进行数据的处理
    val droolsDStream = vehicleDstream.map{rdd =>
      val key = rdd._1
      val vehicle = rdd._2
      //  从Redis获取规则，使用drools执行规则引擎处理数据
      DroolsRedisVehicle.updateVehicleRules(vehicle)
      (key,vehicle)
    }


    droolsDStream.foreachRDD { rdd =>
      rdd.foreachPartition { items =>

        //需要用到连接池技术
        //创建到Kafka的连接
        val pool = KafkaConnPool(targetTopic_brokenList)
        //拿到连接
        val kafkaProxy = pool.borrowObject()

        for(item <- items) {
          val key = item._1
          val vehicle = item._2
          val value = JSON.toJSONString(vehicle,SerializerFeature.WriteDateUseDateFormat)
          //          println("key:  " + key +  ",  value:>>>>>>>>>>>>>:   " + value )
          println("GPS:  " + vehicle.getGpstime + ",lon: " + vehicle.getLon + ",lat: " + vehicle.getLat + ",speaker: " + vehicle.getSpeaker + ",mnetwork: " + vehicle.getMnetworkcode)
          println("#####  开     始  ######")
          println("##########################")
          println("##########################")
          println("##########################")
          println("key: " + item._1 + ",value:" + value)
          println("##########################")
          println("value::::::::::  " + value)
          println("####  结        束   #######")
          kafkaProxy.send(targetTopic, value)
        }
        //返回连接
        pool.returnObject(kafkaProxy)
      }
      //保存Offset到ZK
      val updateTopicDirs = new ZKGroupTopicDirs(groupid, sourceTopic)
      val updateZkClient = new ZkClient(zookeeper)
      for(offset <- offsetRanges){
        //将更新写入到Path
        println(offset)
        val zkPath = s"${updateTopicDirs.consumerOffsetDir}/${offset.partition}"
        ZkUtils.updatePersistentPath(updateZkClient, zkPath, offset.fromOffset.toString)
      }
      //  关闭与zk的会话连接，否则连接数太多，会报异常：
      updateZkClient.close()
    }

    //启动StreamingContext
    ssc.start()
    ssc.awaitTermination()

  }

  //  获取对应的DStream流信息
  def getDStream(sparkConf: SparkConf,ssc:StreamingContext): InputDStream[(String,String)] = {
    //创建Kafka的连接参数
    val kafkaParam = Map[String,String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokenList,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> prop.getProperty("kafka.source.key.deserializer"),
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> prop.getProperty("kafka.source.value.deserializer"),
      ConsumerConfig.GROUP_ID_CONFIG -> groupid,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> prop.getProperty("kafka.source.auto.offset.reset")
    )

    var textKafkaDStream: InputDStream[(String, String)] = null

    //获取ZK中保存group + topic 的路径
    val topicDirs = new ZKGroupTopicDirs(groupid, sourceTopic)
    //最终保存Offset的地方
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    val zkClient = new ZkClient(zookeeper)
    val children = zkClient.countChildren(zkTopicPath)

    // 判ZK中是否有保存的数据
    if (children > 0) {
      //从ZK中获取Offset，根据Offset来创建连接
      var fromOffsets: Map[TopicAndPartition, Long] = Map()

      //首先获取每一个分区的主节点
      val topicList = List(sourceTopic)
      //向Kafka集群获取所有的元信息， 你随便连接任何一个节点都可以
      val request = new TopicMetadataRequest(topicList, 0)
      //  kafka.broker.master.vehicle:  192.168.4.35
      val getLeaderConsumer = new SimpleConsumer(prop.getProperty("kafka.broker.master.vehicle"), 9092, 100000, 10000, "OffsetLookup")
      //该请求包含所有的元信息，主要拿到 分区 -》 主节点
      val response = getLeaderConsumer.send(request)
      val topicMetadataOption = response.topicsMetadata.headOption
      val partitons = topicMetadataOption match {
        case Some(tm) => tm.partitionsMetadata.map(pm => (pm.partitionId, pm.leader.get.host)).toMap[Int, String]
        case None => Map[Int, String]()
      }
      getLeaderConsumer.close()
      println("partitions information is: " + partitons)
      println("children information is: " + children)

      for (i <- 0 until children) {
        //先从ZK读取i这个分区的offset保存
        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        println(s"partition[${i}] 目前的offset是：${partitionOffset}")

        // 从当前i的分区主节点去读最小的offset，
        val tp = TopicAndPartition(sourceTopic, i)
        val requestMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)))
        val consumerMin = new SimpleConsumer(partitons(i), 9092, 10000, 10000, "getMiniOffset")
        val curOffsets = consumerMin.getOffsetsBefore(requestMin).partitionErrorAndOffsets(tp).offsets
        consumerMin.close()

        //合并这两个offset
        var nextOffset = partitionOffset.toLong
        if (curOffsets.length > 0 && nextOffset < curOffsets.head) {
          nextOffset = curOffsets.head
        }

        println(s"Partition[${i}] 修正后的偏移量是：${nextOffset}")
        fromOffsets += (tp -> nextOffset)
      }

      zkClient.close()
      println("从ZK中恢复创建Kafka连接")
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      textKafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParam, fromOffsets, messageHandler)
    } else {
      //直接创建到Kafka的连接
      println("直接创建Kafka连接")
      textKafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, Set(sourceTopic))
    }
    //  返回相应的stream
    textKafkaDStream
  }
*/
}
