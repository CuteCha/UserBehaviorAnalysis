package com.atguigu.networkflow_analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.shaded.guava18.com.google.common.hash.{BloomFilter, Funnels}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{WindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.util.Collector

import java.lang
import java.sql.Timestamp
import redis.clients.jedis.Jedis

object PvUvStata {
  case class UserBehavior(uid: String, sid: String, cid: String, behavior: String, ts: Long)

  case class PvUvCount(sid: String, pv: Long, uv: Long, ts: Long, endDate: Long, cts: Long)

  case class PvUvAcc(sid: String, pv: Long, uids: Set[String], ts: Long)

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
        UserBehavior(arr(0), arr(1), arr(2), arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.ts * 1000L)

    val pvUvStream = dataStream
      .filter(_.behavior == "pv")
      .keyBy(_.sid)
      .timeWindow(Time.seconds(10))
      .aggregate(new PvUvCountAgg(), new PvUvCountAggResult())


    pvUvStream
      //      .filter(r => (r.uv > 1) && (r.pv > r.uv))
      .print()

    env.execute("uv job")

  }


  class PvUvCountAgg() extends AggregateFunction[UserBehavior, PvUvAcc, PvUvAcc] {

    override def createAccumulator(): PvUvAcc = PvUvAcc("", 0, Set(), 0)

    override def add(in: UserBehavior, acc: PvUvAcc): PvUvAcc = {
      val sid = if (acc.sid.isEmpty) in.sid else acc.sid
      PvUvAcc(sid, acc.pv + 1, acc.uids + in.uid, acc.ts.max(in.ts))
    }

    override def getResult(acc: PvUvAcc): PvUvAcc = acc

    override def merge(acc: PvUvAcc, acc1: PvUvAcc): PvUvAcc = {
      val sid = if (acc.sid.isEmpty) acc1.sid else acc.sid
      PvUvAcc(sid, acc.pv + acc1.pv, acc.uids ++ acc1.uids, acc.ts.max(acc1.ts))
    }
  }

  class PvUvCountAggResult() extends WindowFunction[PvUvAcc, PvUvCount, String, TimeWindow] {
    override def apply(key: String,
                       window: TimeWindow,
                       input: Iterable[PvUvAcc],
                       out: Collector[PvUvCount]): Unit = {
      val pvUvAcc = input.head
      val cts = System.currentTimeMillis()
      //println(s"$key - ${pvUvAcc.sid}")
      out.collect(PvUvCount(key, pvUvAcc.pv, pvUvAcc.uids.size, pvUvAcc.ts, window.getEnd, cts))
    }
  }

  def test02(args: Array[String]): Unit = {
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
        UserBehavior(arr(0), arr(1), arr(2), arr(3), arr(4).toLong * 1000L)
      })
      .filter(_.behavior.equals("pv"))
      .assignAscendingTimestamps(_.ts)

    val pvUvStream = dataStream
      .map(r => ("key", r.uid.toLong))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .aggregate(new UvAggFunc, new UvProcessFunc)


    pvUvStream
      //.filter(r => (r.uv > 1) && (r.pv > r.uv))
      .print()

    env.execute("uv job")

  }

  //直接用聚合算子，【count，布隆过滤器】
  class UvAggFunc extends AggregateFunction[(String, Long), (Long, BloomFilter[lang.Long]), Long] {
    override def createAccumulator(): (Long, BloomFilter[lang.Long]) = {
      (0, BloomFilter.create(Funnels.longFunnel(), 100000, 0.01))
    }

    override def add(value: (String, Long),
                     accumulator: (Long, BloomFilter[lang.Long])): (Long, BloomFilter[lang.Long]) = {
      var bloom: BloomFilter[lang.Long] = accumulator._2
      var uvCount = accumulator._1
      //通过布隆过滤器判断是否存在，不存在则+1
      if (!bloom.mightContain(value._2)) {
        bloom.put(value._2)
        uvCount += 1
      }
      (uvCount, bloom)
    }

    override def getResult(accumulator: (Long, BloomFilter[lang.Long])): Long = accumulator._1 //返回count

    override def merge(a: (Long, BloomFilter[lang.Long]),
                       b: (Long, BloomFilter[lang.Long])): (Long, BloomFilter[lang.Long]) = ???
  }

  class UvProcessFunc extends ProcessWindowFunction[Long, String, String, TimeWindow] {
    override def process(key: String,
                         context: Context,
                         elements: Iterable[Long],
                         out: Collector[String]): Unit = {
      // 窗口结束时间 ==> UV数
      // 窗口结束时间 ==> bit数组

      // 拿到key
      val start = new Timestamp(context.window.getStart)
      val end = new Timestamp(context.window.getEnd)
      out.collect(s"窗口开始时间为$start 到 $end 的uv 为 ${elements.head}")
    }

  }

  def test03(args: Array[String]): Unit = {
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
        UserBehavior(arr(0), arr(1), arr(2), arr(3), arr(4).toLong * 1000L)
      })
      .filter(_.behavior.equals("pv"))
      .assignAscendingTimestamps(_.ts)

    val pvUvStream = dataStream
      .map(r => ("key", r.uid.toLong))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new UvTrigger)
      .process(new UvProcessFunc3)


    pvUvStream
      //.filter(r => (r.uv > 1) && (r.pv > r.uv))
      .print()

    env.execute("uv job")

  }

  class UvTrigger extends Trigger[(String, Long), TimeWindow] {
    // 来一条元素调用一次
    override def onElement(element: (String, Long),
                           timestamp: Long,
                           window: TimeWindow,
                           ctx: Trigger.TriggerContext): TriggerResult = {
      // 来一个事件，就触发一次窗口计算，并清空窗口
      TriggerResult.FIRE_AND_PURGE
    }

    override def onProcessingTime(time: Long,
                                  window: TimeWindow,
                                  ctx: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onEventTime(time: Long,
                             window: TimeWindow,
                             ctx: Trigger.TriggerContext): TriggerResult = {
      //窗口关闭是会触发该函数
      val jedis = new Jedis("localhost", 6379)
      val windowEnd = window.getEnd.toString
      //从redis中读取结果并打印
      println(new Timestamp(windowEnd.toLong), jedis.hget("UvCount", windowEnd)) //在这打印时间

      TriggerResult.CONTINUE
    }

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
  }

  class UvProcessFunc3 extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    // 连接到redis，用懒加载，只会加载一次
    lazy val jedis = new Jedis("localhost", 6379)

    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Long)],
                         out: Collector[String]): Unit = {
      //redis存储数据类型
      // 窗口结束时间 ==> UV数
      // 窗口结束时间 ==> bit数组

      // 拿到key
      val windowEnd = context.window.getEnd.toString

      var count = 0L

      if (jedis.hget("UvCount", windowEnd) != null) {
        count = jedis.hget("UvCount", windowEnd).toLong
      }

      // 迭代器中只有一条元素，因为每来一条元素，窗口清空一次，见trigger
      val userId = elements.head._2.toString
      // 计算userId对应的bit数组的下标
      val idx = hash(userId, 1 << 20)

      // 判断userId是否访问过
      if (!jedis.getbit(windowEnd, idx)) { // 对应的bit为0的话，返回false，用户一定没访问过
        jedis.setbit(windowEnd, idx, true) // 将idx对应的bit翻转为1
        jedis.hset("UvCount", windowEnd, (count + 1).toString) //写入结果
      }
    }
  }

  // 为了方便理解，只实现一个哈希函数，返回值是Long，bit数组的下标
  // value: 字符串；size：bit数组的长度
  def hash(value: String, size: Long): Long = {
    val seed = 61 // 种子，必须是质数，能够很好的防止相撞
    var result = 0L
    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i)
    }

    (size - 1) & result
  }


  def main(args: Array[String]): Unit = {
    test02(args)
  }
}
