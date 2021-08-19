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
      .timeWindow(Time.days(1))
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

  case class PvUvRoaringBitmapACC(sid: String, pv: Long, uv: Long, uidRB: RoaringBitmap)

  def uvRoaringBitmapStata(args: Array[String]): Unit = {
    val env = getEnv()
    val dataStream = getDataStream(env)

    val pvUvStream = dataStream
      .keyBy(_.sid)
      .timeWindow(Time.days(1))
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

  def hash(uid: String, size: Long): Long = {
    MurmurHash.hash(uid.getBytes, 127) & (size - 1)
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

  def debug(args: Array[String]): Unit = {
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

  def main(args: Array[String]): Unit = {
    uvSetSata(args)
    uvRoaringBitmapStata(args)
  }

}
