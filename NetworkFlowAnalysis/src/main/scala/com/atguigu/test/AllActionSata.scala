package com.atguigu.test

import com.atguigu.networkflow_analysis.RedisTools
import com.google.gson.Gson
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

import java.io.File
import java.sql.Date
import java.text.SimpleDateFormat
import scala.collection.JavaConverters._
import scala.collection.mutable

object AllActionSata {

  private val gson = new Gson

  case class UserLog(UID: String, SID: String, GID: String, ActionType: String, ActionTime: Long, ActionWeight: Double)

  case class ActionACC(pv: Long, uv: Long, uidSet: Set[String])

  case class ActionOUT(pv: Long, uv: Long)

  case class EEFeatureOUT(dt: String, sid: String, out: mutable.HashMap[String, ActionOUT])

  case class EEFeatureACC(sid: String, acc: mutable.HashMap[String, ActionACC])

  class WindowOUTAgg extends AggregateFunction[EEFeatureOUT, List[EEFeatureOUT], List[EEFeatureOUT]] {
    override def createAccumulator(): List[EEFeatureOUT] = Nil

    override def add(value: EEFeatureOUT, accumulator: List[EEFeatureOUT]): List[EEFeatureOUT] = value :: accumulator

    override def getResult(accumulator: List[EEFeatureOUT]): List[EEFeatureOUT] = accumulator

    override def merge(a: List[EEFeatureOUT], b: List[EEFeatureOUT]): List[EEFeatureOUT] = a ++ b
  }

  class EEFeatureAgg(actionList: List[String]) extends AggregateFunction[UserLog, EEFeatureACC, EEFeatureACC] {

    override def createAccumulator(): EEFeatureACC = {
      val res = mutable.HashMap[String, ActionACC]()
      actionList.foreach(x => {
        res += (x -> ActionACC(0, 0, Set[String]()))
      })

      EEFeatureACC("", res)
    }

    override def add(value: UserLog, accumulator: EEFeatureACC): EEFeatureACC = {
      val sid = if (accumulator.sid.nonEmpty) accumulator.sid else value.SID
      val action = value.ActionType

      if (actionList.contains(action)) {
        val actionACC = accumulator.acc.getOrElse(action, ActionACC(0, 0, Set[String]()))
        accumulator.acc(action) = ActionACC(actionACC.pv + 1, 0, actionACC.uidSet + value.UID)

        EEFeatureACC(sid, accumulator.acc)
      } else {

        accumulator
      }
    }

    override def getResult(accumulator: EEFeatureACC): EEFeatureACC = accumulator

    override def merge(a: EEFeatureACC, b: EEFeatureACC): EEFeatureACC = {
      val sid = if (b.sid.nonEmpty) b.sid else a.sid
      val accKeys = a.acc.keySet ++ b.acc.keySet
      val res = mutable.HashMap[String, ActionACC]()

      accKeys.foreach(x => {
        val aAct = a.acc.getOrElse(x, ActionACC(0, 0, Set[String]()))
        val bAct = b.acc.getOrElse(x, ActionACC(0, 0, Set[String]()))

        res += (x -> ActionACC(aAct.pv + aAct.pv, 0, aAct.uidSet ++ bAct.uidSet))
      })

      EEFeatureACC(sid, res)
    }
  }

  /**
   *
   * @param calSize statistic time interval(minute), must be divisible by windowSize(minute)
   * @param accTTL  single windowSize result ttl
   * @param aggTTL  time interval aggregate ttl
   */

  class EEFeatureProcFunc(calSize: Long, accTTL: Int, aggTTL: Int)
    extends ProcessWindowFunction[EEFeatureACC, EEFeatureOUT, String, TimeWindow] {

    private var sid: String = _
    private var windowEnd: Long = _
    private var latestAggWindowEnds: List[String] = _

    private val acc = "acc"
    private val agg = "agg"

    @transient private var jedis: Jedis = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      jedis = RedisTools.getPool.getResource
    }

    def calActionOUT(record: EEFeatureACC, eventType: String): ActionOUT = {
      val actionACC = record.acc.getOrElse(eventType, ActionACC(0, 0, Set[String]()))
      val currEidSizeSid = s"${eventType}_${calSize}_$sid"
      val currSuffix = s"${currEidSizeSid}_$windowEnd"
      val currAccUvKey = s"${acc}_uv_$currSuffix"
      val currAccPvKey = s"${acc}_pv_$currSuffix"
      val currAggUvKey = s"${agg}_uv_$currSuffix"
      val latestAggUvKeys = latestAggWindowEnds.map(x => s"${acc}_uv_${currEidSizeSid}_$x")
      val latestAggPvKeys = latestAggWindowEnds.map(x => s"${acc}_pv_${currEidSizeSid}_$x")

      jedis.pfadd(currAccUvKey, actionACC.uidSet.toList: _*)
      jedis.set(currAccPvKey, actionACC.pv.toString)
      jedis.pfmerge(currAggUvKey, latestAggUvKeys: _*)

      val uv = jedis.pfcount(currAggUvKey)
      val pv = jedis.mget(latestAggPvKeys: _*).asScala.map(x => if (x == null) 0L else x.toLong).sum

      jedis.expire(currAccUvKey, accTTL)
      jedis.expire(currAccPvKey, accTTL)
      jedis.expire(currAggUvKey, aggTTL)

      ActionOUT(pv, uv)
    }


    override def process(key: String,
                         context: Context,
                         elements: Iterable[EEFeatureACC],
                         out: Collector[EEFeatureOUT]): Unit = {
      val record = elements.head
      val windowStart = context.window.getStart
      windowEnd = context.window.getEnd
      sid = record.sid
      val deltaTime = windowEnd - windowStart
      val dayWindowCnt = calSize / (deltaTime / 1000 / 60)
      latestAggWindowEnds = (0L until dayWindowCnt).toList.map(i => (windowEnd - i * deltaTime).toString)

      val actionList = record.acc.keySet.toList
      val res = mutable.HashMap[String, ActionOUT]()
      actionList.foreach(x => {
        res += (x -> calActionOUT(record, x))
      })

      out.collect(EEFeatureOUT(s"${latestAggWindowEnds.last.toLong - deltaTime}~${windowEnd}", sid, res))
    }
  }


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

  def getDataStream(env: StreamExecutionEnvironment): DataStream[UserLog] = {
    val resource = getClass.getResource("/UserBehavior.csv")
    val inputStream: DataStream[String] = env.readTextFile(resource.getPath)

    val dataStream = inputStream
      .map(line => {
        val arr = line.split(",")
        UserLog(arr(0), arr(1), arr(2), arr(3), arr(4).toLong * 1000L, 1.0d)
      })
      .assignAscendingTimestamps(_.ActionTime)

    dataStream
  }

  def sidEEFeature(args: Array[String]): Unit = {
    val env = getEnv()
    val dataStream = getDataStream(env)
    val windowSize = 5L
    val calSize = 1 * 60L
    val stataAct = List("pv", "buy")

    val pvUvStream = dataStream
      .keyBy(_.SID)
      .timeWindow(Time.minutes(windowSize))
      .aggregate(new EEFeatureAgg(stataAct), new EEFeatureProcFunc(calSize, 3600, 3600))
      .filter(f => f.sid.nonEmpty)
      .timeWindowAll(Time.minutes(windowSize))
      .aggregate(new WindowOUTAgg)


    pvUvStream
      .print()

    val sink: StreamingFileSink[String] = getFileSink("sidEEFeature")
    pvUvStream
      .map(x => gson.toJson(x))
      .addSink(sink)

    env.execute("sidEEFeature job")

  }


  def main(args: Array[String]): Unit = {
    sidEEFeature(args)
  }

}
