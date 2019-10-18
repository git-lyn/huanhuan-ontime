package com.hh.ontime

import java.util.{Calendar, Date, Properties}

import org.apache.kafka.common.serialization.StringDeserializer
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.hh.dao.IndexCountDao
import com.hh.model.IndexClickCount
import com.wmidop.datacollector.util.DateUtils
import org.apache.ivy.util.filter.FilterHelper
import org.apache.spark.streaming.{State, StateSpec}
import org.joda.time.DateTime

import scala.collection.mutable.ArrayBuffer
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

/**
 * 实时统计页面的pv、uv
 */
object OntimeStreaming {
  val stream = OntimeStreaming.getClass.getClassLoader.getResourceAsStream("kafka-config.properties")
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
    while(true) {
      val nowTimes = System.currentTimeMillis();
      val time = new DateTime(nowTimes).toString("yyyy-MM-dd")
     val ssc = StreamingContext.getOrCreate(DateUtils.getCheckpoingDir(),functionToCreateContext)
      ssc.start()
      ssc.awaitTerminationOrTimeout(resetTime)
      // ssc.stop(false,true)表示优雅地销毁StreamingContext对象，不能销毁SparkContext对象，
      // ssc.stop(true,true)会停掉SparkContext对象，程序就直接停了。
      ssc.stop(false,true)
    }
  }

  /**
   * 普通启动方式
   */
  def result():Unit = {

    //创建sparkConf对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("OntimeStreaming")
    //创建StreamingContext对象
    val ssc = new StreamingContext(sparkConf,Seconds(5));
    // 设置检查点
    ssc.checkpoint("./streaming_checkpoint")

    //  从kafka中获取对应的信息流数据
    val textKafkaDStream = getDStream(sparkConf,ssc)

    var offsetRanges = Array[OffsetRange]()
    //注意，要想获得offsetRanges必须作为第一步

    val textKafkaDStream2 = textKafkaDStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    // 得到元组结果集
    val dstream = textKafkaDStream2.map{rdd =>
      val key = rdd._1
      val value = rdd._2
      val jsons = JSON.parseObject(value)
      // 获取十位的时间戳
      val timestamps = jsons.get("timestamp").toString()
      // 转换成13位的时间戳
      val times = new DateTime(timestamps.toLong * 1000)
      val dates = times.toString("yyyy-MM-dd")  // yyyy-MM-dd HH:mm:ss
      val content = jsons.getJSONObject("content")
      val user_id = content.get("user_id")
      val deviceID = content.get("deviceID")
      val gid = content.get("gid")
      val click_position = content.get("click_position")
      // 组合成 2019-09-10_dfdfdoogod-dsfdod--sdf
      val result = dates + "|" + click_position + "|" + gid
      (result,1L)
    }

    val aggregatedDStream = dstream.updateStateByKey[Long]{(values:Seq[Long], old:Option[Long]) =>
      // 举例来说
      // 对于每个key，都会调用一次这个方法
      // 比如key是<20151201_Jiangsu_Nanjing_10001,1>，就会来调用一次这个方法7
      // 10个

      // values，(1,1,1,1,1,1,1,1,1,1)

      // 首先根据optional判断，之前这个key，是否有对应的状态
      var clickCount = 0L

      // 如果说，之前是存在这个状态的，那么就以之前的状态作为起点，进行值的累加
      if(old.isDefined) {
        clickCount = old.get
      }

      // values，代表了，batch rdd中，每个key对应的所有的值
      for(value <- values) {
        clickCount += value
      }

      Some(clickCount)
    }

    // 打印kafka的结果集
    aggregatedDStream.foreachRDD{rdd =>
      // 循环遍历每一个rdd
      rdd.foreachPartition{items =>
        for(item <- items) {
          println("key########: " + item._1 + "     value@@@@@: " + item._2)
          //          println("value@@@@@: " + item._2)
        }
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
//      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> prop.getProperty("kafka.source.key.deserializer"),
//      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> prop.getProperty("kafka.source.value.deserializer"),
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
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

  // 创建和设置一个新的StreamingContext
  def functionToCreateContext(): StreamingContext ={
    val stateSpec = StateSpec.function(mapFunction)
    //创建sparkConf对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("OntimeStreaming")
    //创建StreamingContext对象
    val ssc = new StreamingContext(sparkConf,Seconds(5));
    // 设置检查点
//    ssc.checkpoint("./streaming_checkpoint")
    ssc.checkpoint(DateUtils.getCheckpoingDir())


    //  从kafka中获取对应的信息流数据
//    val textKafkaDStream = getDStream(sparkConf,ssc)

    //创建Kafka的连接参数
    val kafkaParam = Map[String,String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokenList,
      //      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> prop.getProperty("kafka.source.key.deserializer"),
      //      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> prop.getProperty("kafka.source.value.deserializer"),
//      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
//      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> groupid,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> prop.getProperty("kafka.source.auto.offset.reset")
    )

    val textKafkaDStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParam,Set(sourceTopic))

    var offsetRanges = Array[OffsetRange]()
    //注意，要想获得offsetRanges必须作为第一步

    val textKafkaDStream2 = textKafkaDStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    // 进行数据的过滤
    val filterDstream = textKafkaDStream2.filter{dstream =>
      val key = dstream._1
      val value = dstream._2
      val jsons = JSON.parseObject(value)
      val content = jsons.getJSONObject("content")
      val click_position = content.get("click_position")
      var flag = false
      if ("app_restart".equals(click_position)) {
        flag = true
      } else {
        flag = false
      }
      flag
    }

    // 得到元组结果集
    val dstream = filterDstream.map{rdd =>
      val key = rdd._1
      val value = rdd._2
      val jsons = JSON.parseObject(value)
      // 获取十位的时间戳
      val timestamps = jsons.get("timestamp").toString()
      // 转换成13位的时间戳
      val times = new DateTime(timestamps.toLong * 1000)
      val dates = times.toString("yyyy-MM-dd")  // yyyy-MM-dd HH:mm:ss
      val content = jsons.getJSONObject("content")
      val user_id = content.get("user_id")
      val deviceID = content.get("deviceID")
      var gid = content.get("gid")
      if(gid == "" || gid == null) {
        gid = "-99"
      }
      val click_position = content.get("click_position")
      // 组合成 2019-09-10_dfdfdoogod-dsfdod--sdf
      val result = dates + "|" + click_position + "|" + gid
      (result,1L)
    }

    val aggregatedDStream = dstream.updateStateByKey[Long]{(values:Seq[Long], old:Option[Long]) =>
      // 举例来说
      // 对于每个key，都会调用一次这个方法
      // 比如key是<20151201_Jiangsu_Nanjing_10001,1>，就会来调用一次这个方法7
      // 10个

      // values，(1,1,1,1,1,1,1,1,1,1)

      // 首先根据optional判断，之前这个key，是否有对应的状态
      var clickCount = 0L

      // 如果说，之前是存在这个状态的，那么就以之前的状态作为起点，进行值的累加
      if(old.isDefined) {
        clickCount = old.get
      }

      // values，代表了，batch rdd中，每个key对应的所有的值
      for(value <- values) {
        clickCount += value
      }

      Some(clickCount)
    }

    // 打印kafka的结果集
    aggregatedDStream.foreachRDD{rdd =>

      // 批量保存数据到数据库
      val indexCounts = ArrayBuffer[IndexClickCount]()

      // 循环遍历每一个rdd
      rdd.foreachPartition{items =>
        for(item <- items) {
          println("key########: " + item._1 + "     value@@@@@: " + item._2)
          //          println("value@@@@@: " + item._2)
          val result = item._1.split("\\|")
          val date = result(0)
          val position = result(1)
          val userid = result(2)
          val counts = item._2
          indexCounts += IndexClickCount(date,position,userid,counts)
          IndexCountDao.updateBatch(indexCounts.toArray)
        }

      }

//      //保存Offset到ZK
//      val updateTopicDirs = new ZKGroupTopicDirs(groupid, sourceTopic)
//      val updateZkClient = new ZkClient(zookeeper)
//      for(offset <- offsetRanges){
//        //将更新写入到Path
//        println(offset)
//        val zkPath = s"${updateTopicDirs.consumerOffsetDir}/${offset.partition}"
//        ZkUtils.updatePersistentPath(updateZkClient, zkPath, offset.fromOffset.toString)
//      }
//      //  关闭与zk的会话连接，否则连接数太多，会报异常：
//      updateZkClient.close()

    }
    ssc
  }

  // 实时流量状态更新函数
  val mapFunction = (datehour: String, pv: Option[Long], state: State[Long]) => {
    val accuSum = pv.getOrElse(0L) + state.getOption().getOrElse(0L)
    val output = (datehour, accuSum)
    state.update(accuSum)
    output
  }

  // 计算当前时间距离次日零点的时长（毫秒）
  def resetTime = {
    val now = new Date()
    val todayEnd = Calendar.getInstance
    todayEnd.set(Calendar.HOUR_OF_DAY, 23) // Calendar.HOUR 12小时制
    todayEnd.set(Calendar.MINUTE, 59)
    todayEnd.set(Calendar.SECOND, 59)
    todayEnd.set(Calendar.MILLISECOND, 999)
    todayEnd.getTimeInMillis - now.getTime
  }

}
