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

import java.lang
import java.sql.Timestamp

object PvUvBloomFilterStata {

  case class UserBehavior(uid: Long, sid: Long, cid: String, behavior: String, ts: Long)

  case class SidPvUvCnt(sid: Long, pv: Long, uv: Long)

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
      .map(r => (r.sid, r.uid))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .aggregate(new PvUvAggFunc, new PvUvProcFunc)


    pvUvStream
      //.filter(r => (r.uv > 1) && (r.pv > r.uv))
      .print()

    env.execute("uv job")
  }

  class PvUvAggFunc extends AggregateFunction[(Long, Long), (Long, Long, Long, BloomFilter[lang.Long]), SidPvUvCnt] {
    override def createAccumulator(): (Long, Long, Long, BloomFilter[lang.Long]) = {
      (0, 0, 0, BloomFilter.create(Funnels.longFunnel(), 1000, 0.01))
    }

    override def add(in: (Long, Long),
                     acc: (Long, Long, Long, BloomFilter[lang.Long])): (Long, Long, Long, BloomFilter[lang.Long]) = {
      var bloom = acc._4
      var uv = acc._3
      val pv = acc._2 + 1

      if (!bloom.mightContain(in._2)) {
        bloom.put(in._2)
        uv += 1
      }

      (in._1, pv, uv, bloom)
    }

    override def getResult(acc: (Long, Long, Long, BloomFilter[lang.Long])): SidPvUvCnt = {
      SidPvUvCnt(acc._1, acc._2, acc._3)
    }

    override def merge(acc: (Long, Long, Long, BloomFilter[lang.Long]),
                       acc1: (Long, Long, Long, BloomFilter[lang.Long])): (Long, Long, Long, BloomFilter[lang.Long]) = ???
  }

  class PvUvProcFunc extends ProcessWindowFunction[SidPvUvCnt, String, Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: Iterable[SidPvUvCnt], out: Collector[String]): Unit = {
      val start = new Timestamp(context.window.getStart)
      val end = new Timestamp(context.window.getEnd)
      val sidPvUvCnt = elements.head
      out.collect(s"start-$start ~ end-$end: sid-${sidPvUvCnt.sid}; pv-${sidPvUvCnt.pv}; uv-${sidPvUvCnt.uv};")
    }
  }

  def main(args: Array[String]): Unit = {
    test01(args)
  }

}
