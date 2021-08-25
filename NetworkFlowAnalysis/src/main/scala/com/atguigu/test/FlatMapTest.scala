package com.atguigu.test

import com.atguigu.networkflow_analysis.PvUvAllAction.{BUY, CART, EEFeatureAgg, EEFeatureProcFunc, PV, UserBehavior, tsToDt}
import com.atguigu.networkflow_analysis.RedisTools
import com.google.gson.Gson
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis
import redis.clients.util.MurmurHash

import java.io.File
import java.sql.Date
import java.text.SimpleDateFormat
import scala.collection.JavaConverters._

object FlatMapTest {
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
    val calSize = 1 * 60L

    val pvUvStream = dataStream
      .keyBy(_.sid)
      .timeWindow(Time.minutes(windowSize))
      .aggregate(new EEFeatureAgg, new EEFeatureProcFunc(calSize))
      .filter(f => f.sid.nonEmpty)
    //.timeWindowAll(Time.minutes(windowSize))
    //.aggregate(new SidAgg)


    pvUvStream
      //.filter(r => (r.expose.pv > 1))
      .print()

    val sink: StreamingFileSink[String] = getFileSink("sidEEFeature")
    pvUvStream
      //.filter(r => (r.expose.pv > 1) && (r.expose.pv > r.expose.uv))
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

    //lazy val jedis = RedisTools.getPool.getResource
    @transient private var jedis: Jedis = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      jedis = RedisTools.getPool.getResource
    }

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

  class SidAgg extends AggregateFunction[EEFeatureOUT, List[String], List[String]] {
    override def createAccumulator(): List[String] = List[String]()

    override def add(in: EEFeatureOUT, acc: List[String]): List[String] = gson.toJson(in) :: acc

    override def getResult(acc: List[String]): List[String] = acc

    override def merge(acc: List[String], acc1: List[String]): List[String] = acc ++ acc1
  }


  case class UserActLog(uid: String, sid: String, gid: String, actionType: String, actionTime: Long, actionWeight: Double)

  case class UserActCnt(uid: String, actionTime: Long, actionWeight: Double)

  case class UserActCntType(sid: String, actionsTypeMap: Map[String, Array[UserActCnt]])

  case class UserActCntMerge(noMean: String, data: UserActCntType)

  def getDataStream2(env: StreamExecutionEnvironment): DataStream[UserActLog] = {
    val resource = getClass.getResource("/UserBehavior.csv")
    val inputStream: DataStream[String] = env.readTextFile(resource.getPath)
    //val inputStream = env.socketTextStream("localhost", 9999).filter(_.trim.nonEmpty)

    val dataStream = inputStream
      .map(line => {
        val arr = line.split(",")
        UserActLog(arr(0), arr(1), arr(2), arr(3), arr(4).toLong * 1000L, 1.0)
      })
      .assignAscendingTimestamps(_.actionTime)

    dataStream
  }

  def test01(args: Array[String]): Unit = {
    val env = getEnv()
    val dataStream = getDataStream2(env)

    val pvUvStream = dataStream
      .flatMap(f => transform2Cnt(f))
      .keyBy(_.noMean)
      .timeWindow(Time.minutes(60), Time.minutes(5))
      .aggregate(new UserActCntAggregateFunction)
      .flatMap(f => f.toArray.map(x => x._1 + ": " + x._2.toArray.map(y => y._1 + "|" + y._2.map(t => gson.toJson(t)).mkString(",")).mkString(";")))

    pvUvStream.print()

    env.execute("test job")

  }

  def transform2Cnt(log: UserActLog): List[UserActCntMerge] = {
    var res = List[UserActCntMerge]()
    res = log.actionType match {
      case "buy" => List(
        UserActCntMerge("c", UserActCntType(log.sid, Map(log.actionType -> Array(UserActCnt(log.uid, log.actionTime, log.actionWeight)))))
      )
      case "fav" => List(
        UserActCntMerge("c", UserActCntType(log.sid, Map(log.actionType -> Array(UserActCnt(log.uid, log.actionTime, log.actionWeight)))))
      )
      case "pv" => List(
        UserActCntMerge("c", UserActCntType(log.sid, Map(log.actionType -> Array(UserActCnt(log.uid, log.actionTime, log.actionWeight)))))
      )
      case "cart" => List(
        UserActCntMerge("c", UserActCntType(log.sid, Map(log.actionType -> Array(UserActCnt(log.uid, log.actionTime, log.actionWeight)))))
      )
      case _ => List[UserActCntMerge]()
    }

    res
  }

  class UserActCntAggregateFunction extends AggregateFunction[UserActCntMerge, Map[String, Map[String, Array[UserActCnt]]], Map[String, Map[String, Array[UserActCnt]]]] {

    def mapMerge(m1: Map[String, Array[UserActCnt]], m2: Map[String, Array[UserActCnt]]): Map[String, Array[UserActCnt]] = {
      m1 ++ m2.map(x => x._1 -> (x._2 ++ m1.getOrElse(x._1, Array())))
    }

    override def createAccumulator(): Map[String, Map[String, Array[UserActCnt]]] = Map()

    override def add(in: UserActCntMerge, acc: Map[String, Map[String, Array[UserActCnt]]]): Map[String, Map[String, Array[UserActCnt]]] = {
      val cntType: UserActCntType = in.data
      acc ++ Map(cntType.sid -> mapMerge(cntType.actionsTypeMap, acc.getOrElse(cntType.sid, Map())))
    }

    override def getResult(acc: Map[String, Map[String, Array[UserActCnt]]]): Map[String, Map[String, Array[UserActCnt]]] = acc

    override def merge(acc: Map[String, Map[String, Array[UserActCnt]]], acc1: Map[String, Map[String, Array[UserActCnt]]]): Map[String, Map[String, Array[UserActCnt]]] = {
      acc ++ acc1.map(x => x._1 -> mapMerge(x._2, acc.getOrElse(x._1, Map())))
    }
  }

  def test02(args: Array[String]): Unit = {
    val env = getEnv()
    val dataStream = getDataStream(env)
    val windowSize = 5L
    val calSize = 1 * 60L

    val pvUvStream = dataStream
      .keyBy(_.sid)
      .timeWindow(Time.minutes(windowSize))
      .aggregate(new EEFeatureAgg, new EEFeatureProcFunc(calSize))
      .filter(f => f.sid.nonEmpty)
      .timeWindowAll(Time.minutes(windowSize))
      .aggregate(new SidAgg)
      .map(x => x.length + ": " + x.mkString("|"))


    pvUvStream
      //.filter(r => (r.expose.pv > 1))
      .map(x => x.split("\\|").head)
      .print()

    val sink: StreamingFileSink[String] = getFileSink("sidEEFeature")

    pvUvStream
      .map(x => x)
      .addSink(sink)


    env.execute("sidEEFeature job")

  }

  def test03(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(8)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.socketTextStream("localhost", 9999).filter(_.trim.nonEmpty)

    val dataStream = inputStream
      .map(line => {
        val arr = line.split(",")
        (arr(0), arr(1).toLong * 1000L)
      })
      .assignAscendingTimestamps(_._2)
    //.assignTimestampsAndWatermarks(
    //  new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(1)) {
    //    override def extractTimestamp(element: (String, Long)): Long = element._2
    //  }
    //)

    val windowSize = 1L
    val windStream = dataStream
      .timeWindowAll(Time.seconds(windowSize))
      .aggregate(new TestAgg, new TestProcAll)
    //.keyBy(_._1)
    //.timeWindow(Time.seconds(windowSize))
    //.aggregate(new TestAgg, new TestProc)


    dataStream
      .map(f => s"${f._1},${f._2},${tsToDt(f._2)}")
      .print("input->")

    windStream.map(f => f.length + ": " + f.mkString(","))
      .print("output->")

    env.execute("timeWindowAll job")
  }

  class TestAgg extends AggregateFunction[(String, Long), List[String], List[String]] {
    override def createAccumulator(): List[String] = Nil

    override def add(in: (String, Long), acc: List[String]): List[String] = s"${in._1}_${in._2}_${tsToDt(in._2)}" :: acc

    override def getResult(acc: List[String]): List[String] = acc

    override def merge(acc: List[String], acc1: List[String]): List[String] = acc ++ acc1
  }

  class TestProc extends ProcessWindowFunction[List[String], List[String], String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[List[String]], out: Collector[List[String]]): Unit = {
      val windowStart = context.window.getStart
      val windowEnd = context.window.getEnd
      val record = elements.head

      out.collect(s"window(${tsToDt(windowStart)}~${tsToDt(windowEnd)})" :: record)
    }
  }

  class TestProcAll extends ProcessAllWindowFunction[List[String], List[String], TimeWindow] {
    override def process(context: Context, elements: Iterable[List[String]], out: Collector[List[String]]): Unit = {
      val windowStart = context.window.getStart
      val windowEnd = context.window.getEnd
      val record = elements.head

      out.collect(s"${tsToDt(windowStart)}~${tsToDt(windowEnd)}" :: record)
    }
  }


  def test04(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(8)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.socketTextStream("localhost", 9999).filter(_.trim.nonEmpty)

    val dataStream = inputStream
      .map(line => {
        val arr = line.split(",")
        (arr(0), arr(1).toLong * 1000L)
      })
      .assignAscendingTimestamps(_._2)

    val windowSize = 1L
    val windStream = dataStream
      .keyBy(_._1)
      .timeWindow(Time.seconds(windowSize))
      .aggregate(new TestAgg, new TestProc04)
      .timeWindowAll(Time.seconds(windowSize))
      .aggregate(new TestAllAgg)


    dataStream
      .map(f => s"${f._1},${f._2},${tsToDt(f._2)}")
      .print("input->")

    windStream.map(f => f.map(x => s"${x._1}|${x._2.mkString(",")}").mkString(";"))
      .print("output->")

    env.execute("timeWindowAll job")
  }

  class TestProc04 extends ProcessWindowFunction[List[String], (String, List[String]), String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[List[String]], out: Collector[(String, List[String])]): Unit = {
      val windowStart = context.window.getStart
      val windowEnd = context.window.getEnd
      val record = elements.head

      out.collect((s"window01(${tsToDt(windowStart)}~${tsToDt(windowEnd)})", record))
    }
  }

  class TestAllAgg extends AggregateFunction[(String, List[String]), List[(String, List[String])], List[(String, List[String])]] {
    override def createAccumulator(): List[(String, List[String])] = Nil

    override def add(in: (String, List[String]), acc: List[(String, List[String])]): List[(String, List[String])] = in :: acc

    override def getResult(acc: List[(String, List[String])]): List[(String, List[String])] = acc

    override def merge(acc: List[(String, List[String])], acc1: List[(String, List[String])]): List[(String, List[String])] = acc ++ acc1
  }

  def main(args: Array[String]): Unit = {
    test04(args)
    //println(dtToTs("2017-11-26 08:05:00"))
    //println(dtToTs("2017-11-26 09:05:00"))
  }

}
