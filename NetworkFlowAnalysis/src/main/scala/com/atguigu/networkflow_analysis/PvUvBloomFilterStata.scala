package com.atguigu.networkflow_analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.shaded.guava18.com.google.common.hash.{BloomFilter, Funnels}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis
import redis.clients.util.MurmurHash

import java.lang
import java.nio.charset.Charset
import java.sql.Timestamp
import scala.collection.JavaConverters._

object PvUvBloomFilterStata {

  case class UserBehavior(uid: Long, sid: Long, cid: String, behavior: String, ts: Long)

  case class SidPvUvCnt(sid: Long, pv: Long, uv: Long)

  case class PvUvACC(sid: Long, pv: Long, uv: Long, bloom: BloomFilter[lang.Long])

  def test01(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(8)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据并转换成样例类类型，并且提取时间戳设置watermark
    val resource = getClass.getResource("/UserBehavior.csv")
    val inputStream: DataStream[String] = env.readTextFile(resource.getPath)
    //val inputStream = env.socketTextStream("localhost", 9999).filter(_.trim.nonEmpty)

    val dataStream: DataStream[UserBehavior] = inputStream
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong * 1000L)
      })
      .filter(_.behavior == "pv")
      .assignAscendingTimestamps(_.ts)


    val pvUvStream = dataStream
      .keyBy(_.sid)
      .timeWindow(Time.hours(1))
      .aggregate(new PvUvAggFunc, new PvUvProcFunc)


    pvUvStream
      //.filter(r => (r.uv > 1) && (r.pv > r.uv))
      .print()

    env.execute("uv job")
  }

  class PvUvAggFunc extends AggregateFunction[UserBehavior, PvUvACC, SidPvUvCnt] {
    override def createAccumulator(): PvUvACC = {
      PvUvACC(0, 0, 0, BloomFilter.create(Funnels.longFunnel(), 1000, 0.01))
    }

    override def add(in: UserBehavior, acc: PvUvACC): PvUvACC = {
      var bloom = acc.bloom
      var uv = acc.uv
      val pv = acc.pv + 1
      val sid = if (acc.sid == 0) in.sid else acc.sid

      if (!bloom.mightContain(in.uid)) {
        bloom.put(in.uid)
        uv += 1
      }

      PvUvACC(sid, pv, uv, bloom)
    }

    override def getResult(acc: PvUvACC): SidPvUvCnt = {
      SidPvUvCnt(acc.sid, acc.pv, acc.uv)
    }

    override def merge(acc: PvUvACC, acc1: PvUvACC): PvUvACC = ???
  }

  class PvUvProcFunc extends ProcessWindowFunction[SidPvUvCnt, String, Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: Iterable[SidPvUvCnt], out: Collector[String]): Unit = {
      val start = new Timestamp(context.window.getStart)
      val end = new Timestamp(context.window.getEnd)
      val sidPvUvCnt = elements.head
      out.collect(s"start-$start ~ end-$end: sid-${sidPvUvCnt.sid}; pv-${sidPvUvCnt.pv}; uv-${sidPvUvCnt.uv};")
    }
  }

  case class UserBehavior02(uid: String, sid: String, cid: String, behavior: String, ts: Long)

  case class SidPvUvCnt02(sid: String, pv: Long, uv: Long)

  case class PvUvACC02(sid: String, pv: Long, uv: Long, bloom: BloomFilter[lang.String])

  def test02(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(8)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据并转换成样例类类型，并且提取时间戳设置watermark
    val resource = getClass.getResource("/UserBehavior.csv")
    val inputStream: DataStream[String] = env.readTextFile(resource.getPath)
    //val inputStream = env.socketTextStream("localhost", 9999).filter(_.trim.nonEmpty)

    val dataStream = inputStream
      .map(line => {
        val arr = line.split(",")
        UserBehavior02(arr(0), arr(1), arr(2), arr(3), arr(4).toLong * 1000L)
      })
      .filter(_.behavior == "pv")
      .assignAscendingTimestamps(_.ts)


    val pvUvStream = dataStream
      .keyBy(_.sid)
      .timeWindow(Time.hours(1))
      .aggregate(new PvUvAggFunc02, new PvUvProcFunc02)


    pvUvStream
      //.filter(r => (r.uv > 1) && (r.pv > r.uv))
      .print()

    env.execute("uv job")
  }

  class PvUvAggFunc02 extends AggregateFunction[UserBehavior02, PvUvACC02, SidPvUvCnt02] {
    override def createAccumulator(): PvUvACC02 = {
      PvUvACC02("", 0, 0, BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), 1000, 0.01))
    }

    override def add(in: UserBehavior02, acc: PvUvACC02): PvUvACC02 = {
      var bloom = acc.bloom
      var uv = acc.uv
      val pv = acc.pv + 1
      val sid = if (acc.sid.isEmpty) in.sid else acc.sid

      if (!bloom.mightContain(in.uid)) {
        bloom.put(in.uid)
        uv += 1
      }

      PvUvACC02(sid, pv, uv, bloom)
    }

    override def getResult(acc: PvUvACC02): SidPvUvCnt02 = {
      SidPvUvCnt02(acc.sid, acc.pv, acc.uv)
    }

    override def merge(acc: PvUvACC02, acc1: PvUvACC02): PvUvACC02 = ???
  }

  class PvUvProcFunc02 extends ProcessWindowFunction[SidPvUvCnt02, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[SidPvUvCnt02], out: Collector[String]): Unit = {
      val start = new Timestamp(context.window.getStart)
      val end = new Timestamp(context.window.getEnd)
      val sidPvUvCnt = elements.head
      out.collect(s"start-$start ~ end-$end: sid-${sidPvUvCnt.sid}; pv-${sidPvUvCnt.pv}; uv-${sidPvUvCnt.uv};")
    }
  }


  case class PvUvACC03(sid: String, pv: Long, uv: Long, bloom: BloomFilter[lang.String])

  def test03(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(8)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据并转换成样例类类型，并且提取时间戳设置watermark
    val resource = getClass.getResource("/UserBehavior.csv")
    val inputStream: DataStream[String] = env.readTextFile(resource.getPath)
    //val inputStream = env.socketTextStream("localhost", 9999).filter(_.trim.nonEmpty)

    val dataStream = inputStream
      .map(line => {
        val arr = line.split(",")
        UserBehavior02(arr(0), arr(1), arr(2), arr(3), arr(4).toLong * 1000L)
      })
      .filter(_.behavior == "pv")
      .assignAscendingTimestamps(_.ts)


    val pvUvStream = dataStream
      .keyBy(_.sid)
      .timeWindow(Time.hours(1))
      .trigger(new PvUvTrigger)
      .process(new PvUvProcFunc03)


    pvUvStream
      //.filter(r => (r.uv > 1) && (r.pv > r.uv))
      .print()

    env.execute("uv job")
  }

  class PvUvTrigger extends Trigger[UserBehavior02, TimeWindow] {
    val prefix = "uv"
    val bmKey = s"${prefix}_bm"
    val uvKey = s"${prefix}_cnt"

    override def onElement(element: UserBehavior02,
                           timestamp: Long,
                           window: TimeWindow,
                           ctx: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.FIRE_AND_PURGE
    }

    override def onProcessingTime(time: Long,
                                  window: TimeWindow,
                                  ctx: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onEventTime(time: Long, window: TimeWindow,
                             ctx: Trigger.TriggerContext): TriggerResult = {
      //窗口关闭是会触发该函数
      val jedis = new Jedis("localhost", 6379)
      val windowEnd = window.getEnd
      //从redis中读取结果并打印
      val dateTime = new Timestamp(windowEnd)
      val uvMap = jedis.hgetAll(s"${uvKey}_${windowEnd}")
      println(s"\n${dateTime}:\n") //在这打印时间
      uvMap.asScala.foreach(x => {
        println(s"sid:${x._1}; uv:${x._2}\n")
        jedis.expire(s"${bmKey}_${windowEnd}_${x._1}", 10)
      })
      jedis.expire(s"${uvKey}_${windowEnd}", 10)
      println("do something ......")


      TriggerResult.CONTINUE

    }

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
  }

  class PvUvProcFunc03 extends ProcessWindowFunction[UserBehavior02, String, String, TimeWindow] {
    // 连接到redis，用懒加载，只会加载一次
    lazy val jedis = new Jedis("localhost", 6379)
    val prefix = "uv"
    val bmKey = s"${prefix}_bm"
    val uvKey = s"${prefix}_cnt"
    val bitSize = 1 << 20 //redis bitmap offset lt 2^32

    override def process(key: String,
                         context: Context,
                         elements: Iterable[UserBehavior02],
                         out: Collector[String]): Unit = {

      // 迭代器中只有一条元素，因为每来一条元素，窗口清空一次，见trigger
      val userBehavior = elements.head
      val windowEnd = context.window.getEnd
      val uvHKey = s"${uvKey}_${windowEnd}"
      val bmHKey = s"${bmKey}_${windowEnd}_${userBehavior.sid}"

      val uvStr = jedis.hget(uvHKey, userBehavior.sid)
      val uv = if (uvStr != null) uvStr.toLong else 0L

      // 计算userId对应的bit数组的下标
      val offset = hash(userBehavior.uid, bitSize)
      print(s"offset: $offset; uid: ${userBehavior.uid}\n")

      // 判断userId是否访问过
      if (!jedis.getbit(bmHKey, offset)) { // 对应的bit为0的话，返回false，用户一定没访问过
        jedis.setbit(bmHKey, offset, true) // 将idx对应的bit翻转为1
        jedis.hset(uvHKey, userBehavior.sid, (uv + 1).toString) //写入结果
      }

    }
  }

  def hash(uid: String, size: Long): Long = {
    MurmurHash.hash(uid.getBytes, 127) & (size - 1)
  }

  def test031(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(8)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据并转换成样例类类型，并且提取时间戳设置watermark
    val resource = getClass.getResource("/UserBehavior.csv")
    val inputStream: DataStream[String] = env.readTextFile(resource.getPath)
    //val inputStream = env.socketTextStream("localhost", 9999).filter(_.trim.nonEmpty)

    val dataStream = inputStream
      .map(line => {
        val arr = line.split(",")
        UserBehavior02(arr(0), arr(1), arr(2), arr(3), arr(4).toLong * 1000L)
      })
      .filter(_.behavior == "pv")
      .assignAscendingTimestamps(_.ts)


    val pvUvStream = dataStream
      .keyBy(_.sid)
      .timeWindow(Time.hours(1))
      .trigger(new PvUvTrigger31)
      .process(new PvUvProcFunc031)


    pvUvStream
      //.filter(r => (r.uv > 1) && (r.pv > r.uv))
      .print()

    env.execute("uv job")
  }

  class PvUvTrigger31 extends Trigger[UserBehavior02, TimeWindow] {
    val prefix = "uv"
    val bmKey = s"${prefix}_bm"
    val uvKey = s"${prefix}_cnt"

    override def onElement(element: UserBehavior02,
                           timestamp: Long,
                           window: TimeWindow,
                           ctx: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.FIRE_AND_PURGE
    }

    override def onProcessingTime(time: Long,
                                  window: TimeWindow,
                                  ctx: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onEventTime(time: Long, window: TimeWindow,
                             ctx: Trigger.TriggerContext): TriggerResult = {
      //窗口关闭是会触发该函数
      //val jedis = new Jedis("localhost", 6379)
      val jedis = RedisTools.getPool.getResource
      val windowEnd = window.getEnd
      //从redis中读取结果并打印
      val dateTime = new Timestamp(windowEnd)
      val uvMap = jedis.hgetAll(s"${uvKey}_${windowEnd}")
      //println(s"\n${dateTime}:\n") //在这打印时间
      uvMap.asScala.par.foreach(x => {
        //println(s"sid:${x._1}; uv:${x._2}\n")
        jedis.expire(s"${bmKey}_${windowEnd}_${x._1}", 10)
      })
      jedis.expire(s"${uvKey}_${windowEnd}", 10)
      //println("do something ......")


      TriggerResult.CONTINUE

    }

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
  }

  class PvUvProcFunc031 extends ProcessWindowFunction[UserBehavior02, String, String, TimeWindow] {
    // 连接到redis，用懒加载，只会加载一次
    //lazy val jedis = new Jedis("localhost", 6379)
    lazy val jedis = RedisTools.getPool.getResource
    val prefix = "uv"
    val bmKey = s"${prefix}_bm"
    val uvKey = s"${prefix}_cnt"
    val bitSize = 1 << 20 //redis bitmap offset lt 2^32

    override def process(key: String,
                         context: Context,
                         elements: Iterable[UserBehavior02],
                         out: Collector[String]): Unit = {

      // 迭代器中只有一条元素，因为每来一条元素，窗口清空一次，见trigger
      val userBehavior = elements.head
      val windowEnd = context.window.getEnd
      val uvHKey = s"${uvKey}_${windowEnd}"
      val bmHKey = s"${bmKey}_${windowEnd}_${userBehavior.sid}"

      val uvStr = jedis.hget(uvHKey, userBehavior.sid)
      val uv = if (uvStr != null) uvStr.toLong else 0L

      val isExist = jedis.sadd(bmHKey, userBehavior.uid)

      if (isExist == 1) {
        jedis.hset(uvHKey, userBehavior.sid, (uv + 1).toString)
      }

    }
  }

  def test04(args: Array[String]): Unit = {
    lazy val jedis = new Jedis("localhost", 6379)
    val prefix = "uv"
    val sid = "s1"
    val uid = "u1"
    val bitSize = 1 << 20
    val offset = hash(uid, bitSize)
    print(s"offset: $offset")

    val bmKey = s"${prefix}_bm"
    val uvKey = s"${prefix}_cnt"

    var uv = 0L
    val uvStr = jedis.hget(uvKey, sid)
    if (uvStr != null) {
      uv = uvStr.toLong
    }

    if (!jedis.getbit(bmKey, offset)) {
      jedis.setbit(bmKey, offset, true)
      jedis.hset(uvKey, sid, (uv + 1).toString)
    }

    jedis.expire(bmKey, 30)
    jedis.expire(uvKey, 30)

  }

  def test05(args: Array[String]): Unit = {
    lazy val jedis = new Jedis("localhost", 6379)
    val prefix = "uv"
    val sid = "s1"
    val uid = "u2"

    val bmKey = s"${prefix}_bm"
    val uvKey = s"${prefix}_cnt"

    var uv = 0L
    val uvStr = jedis.hget(uvKey, sid)
    if (uvStr != null) {
      uv = uvStr.toLong
    }

    val isExist = jedis.sadd(bmKey, uid)

    if (isExist == 1) {
      jedis.hset(uvKey, sid, (uv + 1).toString)
    }

    //jedis.expire(bmKey, 30)
    //jedis.expire(uvKey, 30)

  }

  def debug(args: Array[String]): Unit = {
    val jedis = new Jedis("localhost", 6379)
    val uvMap = jedis.hgetAll("uv_cnt")
    uvMap.asScala.foreach(x => println(s"sid:${x._1}; uv: ${x._2}"))
    println(uvMap.get("s1"))
    println(math.pow(2, 20))
    println(1 << 20)
  }

  def main(args: Array[String]): Unit = {
    test031(args)
  }

}
