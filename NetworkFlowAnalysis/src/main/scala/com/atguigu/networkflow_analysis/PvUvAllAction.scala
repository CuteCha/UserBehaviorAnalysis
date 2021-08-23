package com.atguigu.networkflow_analysis

import com.google.gson.Gson
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis
import redis.clients.util.MurmurHash

import java.io.File
import java.sql.Date
import java.text.SimpleDateFormat
import scala.collection.JavaConverters._
import scala.io.Source

object PvUvAllAction {
  val PV: String = "pv"
  val CART: String = "cart"
  val FAV: String = "fav"
  val BUY: String = "buy"

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
      .assignAscendingTimestamps(_.ts)

    dataStream
  }


  def sidEEFeature(args: Array[String]): Unit = {
    val env = getEnv()
    val dataStream = getDataStream(env)
    val windowSize = 5L
    val calSize = 24 * 60L

    val pvUvStream = dataStream
      .keyBy(_.sid)
      .timeWindow(Time.minutes(windowSize))
      .aggregate(new EEFeatureAgg, new EEFeatureProcFunc(calSize))


    pvUvStream
      .filter(r => (r.expose.pv > 1) && (r.expose.pv > r.expose.uv))
      .print()

    val sink: StreamingFileSink[String] = getFileSink("sidEEFeature")
    pvUvStream
      .filter(r => (r.expose.pv > 1) && (r.expose.pv > r.expose.uv))
      .map(x => gson.toJson(x))
      .addSink(sink)

    env.execute("sidEEFeature job")

  }

  case class ActionACC(pv: Long, uv: Long, uidSet: Set[String])

  case class ActionOUT(pv: Long, uv: Long)

  case class EEFeatureACC(sid: String, expose: ActionACC, click: ActionACC, watch: ActionACC)

  case class EEFeatureOUT(dt: String, sid: String, expose: ActionOUT, click: ActionOUT, watch: ActionOUT)

  class EEFeatureAgg extends AggregateFunction[UserBehavior, EEFeatureACC, EEFeatureACC] {
    override def createAccumulator(): EEFeatureACC = {
      EEFeatureACC("", ActionACC(0, 0, Set[String]()), ActionACC(0, 0, Set[String]()), ActionACC(0, 0, Set[String]()))
    }

    override def add(value: UserBehavior, accumulator: EEFeatureACC): EEFeatureACC = {
      val sid = if (accumulator.sid.nonEmpty) accumulator.sid else value.sid
      value.behavior match {
        case PV => {
          val actionACC = ActionACC(accumulator.expose.pv + 1, 0, accumulator.expose.uidSet + value.uid)

          EEFeatureACC(sid, actionACC, accumulator.click, accumulator.watch)
        }
        case CART => {
          val actionACC = ActionACC(accumulator.click.pv + 1, 0, accumulator.click.uidSet + value.uid)

          EEFeatureACC(sid, accumulator.expose, actionACC, accumulator.watch)
        }
        case BUY => {
          val actionACC = ActionACC(accumulator.watch.pv + 1, 0, accumulator.watch.uidSet + value.uid)

          EEFeatureACC(sid, accumulator.expose, accumulator.click, actionACC)
        }
        case _ => accumulator
      }
    }

    override def getResult(accumulator: EEFeatureACC): EEFeatureACC = accumulator

    override def merge(a: EEFeatureACC, b: EEFeatureACC): EEFeatureACC = {
      val sid = if (b.sid.nonEmpty) b.sid else a.sid
      EEFeatureACC(sid,
        ActionACC(a.expose.pv + b.expose.pv, 0, a.expose.uidSet ++ b.expose.uidSet),
        ActionACC(a.click.pv + b.click.pv, 0, a.click.uidSet ++ b.click.uidSet),
        ActionACC(a.watch.pv + b.watch.pv, 0, a.watch.uidSet ++ b.watch.uidSet))
    }
  }

  class EEFeatureProcFunc(calSize: Long = 24 * 60) //multiple of window size
    extends ProcessWindowFunction[EEFeatureACC, EEFeatureOUT, String, TimeWindow] {
    val ttlMin = 36 * 60 * 60
    val ttlDay = 1 * 60 * 60

    var sid: String = _
    var windowEnd: Long = _
    var latestAggWindowEnds: List[String] = _

    lazy val jedis = RedisTools.getPool.getResource

    def calActionOUT(actionACC: ActionACC, eventType: String): ActionOUT = {
      val currEidSizeSid = s"${eventType}_${calSize}_${sid}"
      val currSuffix = s"${currEidSizeSid}_${windowEnd}"
      val currMinUvKey = s"min_uv_${currSuffix}"
      val currMinPvKey = s"min_pv_${currSuffix}"
      val currAggUvKey = s"agg_uv_${currSuffix}"
      val latestAggUvKeys = latestAggWindowEnds.map(x => s"min_uv_${currEidSizeSid}_${x}")
      val latestAggPvKeys = latestAggWindowEnds.map(x => s"min_pv_${currEidSizeSid}_${x}")

      jedis.pfadd(currMinUvKey, actionACC.uidSet.toList: _*)
      jedis.set(currMinPvKey, actionACC.pv.toString)
      jedis.pfmerge(currAggUvKey, latestAggUvKeys: _*)

      val uv = jedis.pfcount(currAggUvKey)
      val pv = jedis.mget(latestAggPvKeys: _*).asScala.map(x => if (x == null) 0L else x.toLong).sum

      jedis.expire(currMinUvKey, ttlMin)
      jedis.expire(currMinPvKey, ttlMin)
      jedis.expire(currAggUvKey, ttlDay)

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
      val dayStart = latestAggWindowEnds.last.toLong - deltaTime

      out.collect(EEFeatureOUT(tsToDt(dayStart) + " ~ " + tsToDt(windowEnd), sid,
        calActionOUT(record.expose, PV),
        calActionOUT(record.click, CART),
        calActionOUT(record.watch, BUY)
      ))


    }
  }

  def check(args: Array[String]): Unit = {
    //EEFeatureOUT(2017-11-25 13:55:00 ~ 2017-11-26 13:55:00,1903537,ActionOUT(11,9),ActionOUT(1,1),ActionOUT(0,0))
    //EEFeatureOUT(2017-11-25 13:55:00 ~ 2017-11-26 13:55:00,3594695,ActionOUT(3,2),ActionOUT(0,0),ActionOUT(1,1))
    //EEFeatureOUT(2017-11-25 14:00:00 ~ 2017-11-26 14:00:00,1559657,ActionOUT(3,2),ActionOUT(0,0),ActionOUT(1,1))
    //EEFeatureOUT(2017-11-25 14:05:00 ~ 2017-11-26 14:05:00,3765552,ActionOUT(2,1),ActionOUT(0,0),ActionOUT(0,0))
    //EEFeatureOUT(2017-11-25 14:05:00 ~ 2017-11-26 14:05:00,1699047,ActionOUT(5,4),ActionOUT(0,0),ActionOUT(1,1))
    //EEFeatureOUT(2017-11-25 14:00:00 ~ 2017-11-26 14:00:00,4846106,ActionOUT(4,3),ActionOUT(0,0),ActionOUT(0,0))
    //EEFeatureOUT(2017-11-25 14:00:00 ~ 2017-11-26 14:00:00,61164,ActionOUT(3,2),ActionOUT(0,0),ActionOUT(0,0))
    //EEFeatureOUT(2017-11-25 14:00:00 ~ 2017-11-26 14:00:00,351492,ActionOUT(2,1),ActionOUT(0,0),ActionOUT(0,0))
    //EEFeatureOUT(2017-11-25 14:00:00 ~ 2017-11-26 14:00:00,2338453,ActionOUT(127,126),ActionOUT(6,6),ActionOUT(5,5))

    val startTime = dtToTs("2017-11-25 14:00:00")
    val endTime = dtToTs("2017-11-26 14:00:00")
    val resource = getClass.getResource("/UserBehavior.csv")
    val res = Source.fromFile(resource.getPath).getLines()
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0), arr(1), arr(2), arr(3), arr(4).toLong * 1000L)
      })
      .filter(r => r.behavior.equals("pv") && r.sid.equals("2338453") && (r.ts >= startTime && r.ts < endTime))

    val res2 = res.toList
    println(endTime)
    println(res2)
    println(res2.size)
    println(res2.map(_.uid).distinct.size)
  }

  def main(args: Array[String]): Unit = {
    sidEEFeature(args)
  }
}
