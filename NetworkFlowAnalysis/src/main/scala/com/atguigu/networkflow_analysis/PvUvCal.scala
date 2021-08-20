package com.atguigu.networkflow_analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.guava18.com.google.common.hash.{BloomFilter, Funnels}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis
import redis.clients.util.MurmurHash

import java.lang
import java.nio.charset.Charset
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import com.google.gson.Gson
import org.apache.flink.core.fs.Path
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.roaringbitmap.RoaringBitmap
import net.agkn.hll.HLL

import java.io.File
import scala.collection.JavaConverters._

object PvUvCal {
  val gson = new Gson

  case class UserBehavior(uid: String, sid: String, cid: String, behavior: String, ts: Long)

  case class PvUvOUT(dt: String, sid: String, pv: Long, uv: Long)

  def tsToDt(ts: Long): String = {
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(ts))
  }

  def dtToTs(dt: String): Long = {
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dt).getTime
  }

  def hash(uid: String, size: Long): Long = {
    MurmurHash.hash(uid.getBytes, 127) & (size - 1)
  }

  def getOutPath(dir: String): String = {
    val file = new File(s"logs/${dir}")

    file.getAbsolutePath
  }

  def getFileSink(dir: String): StreamingFileSink[String] = {
    val outputDir = getOutPath(dir)
    val sink: StreamingFileSink[String] = StreamingFileSink
      .forRowFormat(new Path(outputDir),
        new SimpleStringEncoder[String]("UTF-8"))
      .build()

    sink
  }

  def getEnv(): StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(8)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env
  }

  def getDataStream(env: StreamExecutionEnvironment): DataStream[UserBehavior] = {
    // 读取数据并转换成样例类类型，并且提取时间戳设置watermark
    val resource = getClass.getResource("/UserBehavior.csv")
    val inputStream: DataStream[String] = env.readTextFile(resource.getPath)
    //val inputStream = env.socketTextStream("localhost", 9999).filter(_.trim.nonEmpty)

    val dataStream = inputStream
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0), arr(1), arr(2), arr(3), arr(4).toLong * 1000L)
      })
      .filter(_.behavior == "pv")
      .assignAscendingTimestamps(_.ts)

    dataStream
  }

  case class PvUvSetACC(sid: String, pv: Long, uv: Long, uidSet: Set[String])

  def uvSetSata(args: Array[String]): Unit = {
    val env = getEnv()
    val dataStream = getDataStream(env)

    val pvUvStream = dataStream
      .keyBy(_.sid)
      .timeWindow(Time.minutes(30), Time.minutes(5))
      .aggregate(new PvUvSetAggFunc, new PvUvResWindowFunc)


    pvUvStream
      .filter(r => (r.uv > 1) && (r.pv > r.uv))
      .print()

    val sink: StreamingFileSink[String] = getFileSink("uvSetSata")
    pvUvStream
      .filter(r => (r.uv > 1) && (r.pv > r.uv))
      .map(x => gson.toJson(x))
      .addSink(sink)

    env.execute("uv job")

  }

  class PvUvSetAggFunc extends AggregateFunction[UserBehavior, PvUvSetACC, PvUvOUT] {
    override def createAccumulator(): PvUvSetACC = PvUvSetACC("", 0, 0, Set[String]())

    override def add(in: UserBehavior, acc: PvUvSetACC): PvUvSetACC = {
      val sid = if (acc.sid.nonEmpty) acc.sid else in.sid
      val uidSet = acc.uidSet + in.uid

      PvUvSetACC(sid, acc.pv + 1, uidSet.size, uidSet)
    }

    override def getResult(acc: PvUvSetACC): PvUvOUT = PvUvOUT("", acc.sid, acc.pv, acc.uv)

    override def merge(acc: PvUvSetACC, acc1: PvUvSetACC): PvUvSetACC = {
      val sid = if (acc.sid.nonEmpty) acc.sid else acc1.sid
      val uidSet = acc.uidSet ++ acc1.uidSet

      PvUvSetACC(sid, acc.pv + acc1.pv, uidSet.size, uidSet)
    }
  }

  class PvUvResWindowFunc extends WindowFunction[PvUvOUT, PvUvOUT, String, TimeWindow] {
    override def apply(key: String,
                       window: TimeWindow,
                       input: Iterable[PvUvOUT],
                       out: Collector[PvUvOUT]): Unit = {
      val windowEnd = window.getEnd
      val dt = tsToDt(windowEnd)
      val record = input.head

      out.collect(PvUvOUT(dt, record.sid, record.pv, record.uv))
    }
  }

  class PvUvResProcWindowFunc extends ProcessWindowFunction[PvUvOUT, PvUvOUT, String, TimeWindow] {
    override def process(key: String,
                         context: Context,
                         elements: Iterable[PvUvOUT],
                         out: Collector[PvUvOUT]): Unit = {
      val windowEnd = context.window.getEnd
      val dt = tsToDt(windowEnd)
      val record = elements.head

      out.collect(PvUvOUT(dt, record.sid, record.pv, record.uv))
    }
  }

  case class PvUvRoaringBitmapACC(sid: String, pv: Long, uv: Long, uidRB: RoaringBitmap)

  def uvRoaringBitmapStata(args: Array[String]): Unit = {
    val env = getEnv()
    val dataStream = getDataStream(env)

    val pvUvStream = dataStream
      .keyBy(_.sid)
      .timeWindow(Time.minutes(30), Time.minutes(5))
      .aggregate(new PvUvRoaringBitmapAggFunc, new PvUvResWindowFunc)


    pvUvStream
      .filter(r => (r.uv > 1) && (r.pv > r.uv))
      .print()

    val sink: StreamingFileSink[String] = getFileSink("uvRoaringBitmapStata")
    pvUvStream
      .filter(r => (r.uv > 1) && (r.pv > r.uv))
      .map(x => gson.toJson(x))
      .addSink(sink)

    env.execute("uv job")

  }

  class PvUvRoaringBitmapAggFunc extends AggregateFunction[UserBehavior, PvUvRoaringBitmapACC, PvUvOUT] {
    val bmSize = 1 << 25

    override def createAccumulator(): PvUvRoaringBitmapACC = PvUvRoaringBitmapACC("", 0, 0, new RoaringBitmap())

    override def add(in: UserBehavior, acc: PvUvRoaringBitmapACC): PvUvRoaringBitmapACC = {
      val sid = if (acc.sid.nonEmpty) acc.sid else in.sid
      var uv = acc.uv
      val idx = hash(in.uid, bmSize).toInt

      if (!acc.uidRB.contains(idx)) {
        acc.uidRB.add(idx)
        uv += 1
      }

      PvUvRoaringBitmapACC(sid, acc.pv + 1, uv, acc.uidRB)
    }

    override def getResult(acc: PvUvRoaringBitmapACC): PvUvOUT = PvUvOUT("", acc.sid, acc.pv, acc.uv)

    override def merge(acc: PvUvRoaringBitmapACC, acc1: PvUvRoaringBitmapACC): PvUvRoaringBitmapACC = {
      val sid = if (acc.sid.nonEmpty) acc.sid else acc1.sid
      val tmpRBM = RoaringBitmap.or(acc.uidRB, acc1.uidRB)

      PvUvRoaringBitmapACC(sid, acc.pv + acc1.pv, tmpRBM.getLongCardinality, tmpRBM)

    }
  }

  case class PvUvHyperLogLogACC(sid: String, pv: Long, uv: Long, uidHLL: HLL)

  def uvHyperLogLogStata(args: Array[String]): Unit = {
    val env = getEnv()
    val dataStream = getDataStream(env)

    val pvUvStream = dataStream
      .keyBy(_.sid)
      .timeWindow(Time.minutes(30), Time.minutes(5))
      .aggregate(new PvUvHyperLogLogAggFunc, new PvUvResWindowFunc)


    pvUvStream
      .filter(r => (r.uv > 1) && (r.pv > r.uv))
      .print()

    val sink: StreamingFileSink[String] = getFileSink("uvHyperLogLogStata")
    pvUvStream
      .filter(r => (r.uv > 1) && (r.pv > r.uv))
      .map(x => gson.toJson(x))
      .addSink(sink)

    env.execute("uv job")

  }

  class PvUvHyperLogLogAggFunc extends AggregateFunction[UserBehavior, PvUvHyperLogLogACC, PvUvOUT] {
    private val bmSize = 1 << 25
    private val log2m = 14
    private val regWidth = 6

    override def createAccumulator(): PvUvHyperLogLogACC = PvUvHyperLogLogACC("", 0, 0, new HLL(log2m, regWidth))

    override def add(in: UserBehavior, acc: PvUvHyperLogLogACC): PvUvHyperLogLogACC = {
      val sid = if (acc.sid.nonEmpty) acc.sid else in.sid
      val idx = hash(in.uid, bmSize).toInt
      acc.uidHLL.addRaw(idx)

      PvUvHyperLogLogACC(sid, acc.pv + 1, acc.uidHLL.cardinality(), acc.uidHLL)
    }

    override def getResult(acc: PvUvHyperLogLogACC): PvUvOUT = PvUvOUT("", acc.sid, acc.pv, acc.uv)

    override def merge(acc: PvUvHyperLogLogACC, acc1: PvUvHyperLogLogACC): PvUvHyperLogLogACC = {
      val sid = if (acc.sid.nonEmpty) acc.sid else acc1.sid
      acc.uidHLL.union(acc1.uidHLL)

      PvUvHyperLogLogACC(sid, acc.pv + acc1.pv, acc.uidHLL.cardinality(), acc.uidHLL)
    }
  }

  case class PvUvRedisHLLACC(sid: String, pv: Long, uv: Long, uidSet: Set[String])

  case class PvUvRedisHLLOUT(sid: String, pv: Long, uv: Long, uidSet: Set[String])

  def uvRedisHLLStata(args: Array[String]): Unit = {
    val env = getEnv()
    val dataStream = getDataStream(env)

    val pvUvStream = dataStream
      .keyBy(_.sid)
      .timeWindow(Time.minutes(5))
      .aggregate(new PvUvRedisHLLAggFunc, new PvUvRedisProcWindowFunc)


    pvUvStream
      .filter(r => (r.uv > 1) && (r.pv > r.uv))
      .print()

    val sink: StreamingFileSink[String] = getFileSink("uvRedisHLLStata")
    pvUvStream
      //.filter(r => (r.uv > 1) && (r.pv > r.uv))
      .map(x => gson.toJson(x))
      .addSink(sink)

    env.execute("uv job")

  }

  class PvUvRedisHLLAggFunc extends AggregateFunction[UserBehavior, PvUvRedisHLLACC, PvUvRedisHLLOUT] {
    override def createAccumulator(): PvUvRedisHLLACC = PvUvRedisHLLACC("", 0, 0, Set[String]())

    override def add(in: UserBehavior, acc: PvUvRedisHLLACC): PvUvRedisHLLACC = {
      val sid = if (acc.sid.nonEmpty) acc.sid else in.sid
      PvUvRedisHLLACC(sid, acc.pv + 1, 0, acc.uidSet + in.uid)
    }

    override def getResult(acc: PvUvRedisHLLACC): PvUvRedisHLLOUT = PvUvRedisHLLOUT(acc.sid, acc.pv, 0, acc.uidSet)

    override def merge(acc: PvUvRedisHLLACC, acc1: PvUvRedisHLLACC): PvUvRedisHLLACC = {
      val sid = if (acc.sid.nonEmpty) acc.sid else acc1.sid
      PvUvRedisHLLACC(sid, acc.pv + acc1.pv, 0, acc.uidSet ++ acc1.uidSet)
    }
  }

  class PvUvRedisProcWindowFunc extends ProcessWindowFunction[PvUvRedisHLLOUT, PvUvOUT, String, TimeWindow] {
    lazy val jedis = RedisTools.getPool.getResource
    val ttlMin = 24 * 60 * 60
    val ttlDay = 1 * 60 * 60

    override def process(key: String,
                         context: Context,
                         elements: Iterable[PvUvRedisHLLOUT],
                         out: Collector[PvUvOUT]): Unit = {
      val windowStart = context.window.getStart
      val windowEnd = context.window.getEnd
      val record = elements.head
      val sid = record.sid
      val deltaTime = windowEnd - windowStart
      val dayWindowCnt = 24 * 60 / deltaTime
      val latestDayWindowEnds = (0L until dayWindowCnt).map(i => (windowEnd - i * deltaTime).toString)
      val latestDayUvKeys = latestDayWindowEnds.map(x => s"${sid}_${x}_min_uv")

      val currPrefix = s"${sid}_${windowEnd}"
      val currMinUvKey = s"${currPrefix}_min_uv"
      val currMinPvKey = s"${currPrefix}_min_pv"
      val currDayUvKey = s"${currPrefix}_day_uv"

      jedis.pfadd(currMinUvKey, record.uidSet.toList: _*)
      jedis.set(currMinPvKey, record.pv.toString)

      jedis.pfmerge(currDayUvKey, latestDayUvKeys.toList: _*)
      val uv = jedis.pfcount(currDayUvKey)
      val pv = latestDayWindowEnds.map(x => {
        val tpv = jedis.get(s"${sid}_${x}_min_pv")
        if (tpv == null) 0L else tpv.toLong
      }).sum

      jedis.expire(currMinUvKey, ttlMin)
      jedis.expire(currMinPvKey, ttlMin)
      jedis.expire(currDayUvKey, ttlDay)

      out.collect(PvUvOUT(tsToDt(windowStart) + " ~ " + tsToDt(windowEnd), sid, pv, uv))

    }
  }


  def rmbDebug(args: Array[String]): Unit = {
    val rmp = new RoaringBitmap()
    rmp.add(1000L, 1200L)
    //println(rmp.select(3))
    //println(rmp.contains(7))
    println(rmp.contains(1001))
    println("-" + rmp.add(1001))
    println(rmp.contains(1001))
    //rmp.add(1)
    //println(rmp.select(3))

    val rmp2 = new RoaringBitmap()
    rmp2.add(900L, 1100L)

    val rmp3 = RoaringBitmap.or(rmp, rmp2)
    //val rmp4 = rmp.and(rmp2)

    println(rmp3.contains(900))
    println(rmp3.contains(1101))

    println(rmp.getLongCardinality)
    println(rmp2.getLongCardinality)
    println(rmp3.getLongCardinality)
    //println(rmp4.getLongCardinality)
  }

  def hllDebug01(args: Array[String]): Unit = {
    val hll = new HLL(14, 6)
    hll.addRaw(1001L)
    println(hll.cardinality())

    hll.addRaw(1001L)
    println(hll.cardinality())

    val hll2 = new HLL(14, 6)
    hll2.addRaw(2001L)

    hll.union(hll2)

    println(hll.cardinality())
  }

  def hllDebug02(args: Array[String]): Unit = {
    val jedis = RedisTools.getPool.getResource

    jedis.pfadd("s1_dt0", "u01", "u02", "u05", "u03", "u01", "u07", "u02")
    jedis.pfadd("s1_dt1", "u11", "u19", "u15", "u11", "u11", "u17", "u02")
    jedis.pfadd("s2_dt1", "u03", "u09", "u05", "u01", "u01", "u07", "u02")

    jedis.pfmerge("s1", "s1_dt0", "s1_dt1")

    jedis.expire("s1_dt0", 100)
    jedis.expire("s1_dt1", 100)
    jedis.expire("s2_dt1", 100)
    jedis.expire("s1", 100)

    println(s"s1_dt0: ${jedis.pfcount("s1_dt0")}")
    println(s"s1_dt1: ${jedis.pfcount("s1_dt1")}")
    println(s"s1: ${jedis.pfcount("s1")}")

    val s1d1 = Set("u01", "u02", "u05", "u03", "u01", "u07", "u02")
    val s1d2 = Set("u11", "u19", "u15", "u11", "u11", "u17", "u02")

    //s1d1.foreach(x => jedis.pfadd("s1d1", x))
    s1d2.foreach(x => jedis.pfadd("s1d2", x))

    jedis.pfadd("s1d1", s1d1.toList: _*)

    val valKeys = List("s1d1", "s1d2")
    //valKeys.foreach(x => jedis.pfmerge("s1", x))
    jedis.pfmerge("s1", valKeys: _*)
    valKeys.foreach(x => println(jedis.pfcount(x)))
    println(jedis.pfcount("s1"))


  }


  def main(args: Array[String]): Unit = {
    uvRedisHLLStata(args)
  }

}
