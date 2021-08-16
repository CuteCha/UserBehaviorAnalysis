package com.atguigu.networkflow_analysis

import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

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
    override def apply(key: String, window: TimeWindow, input: Iterable[PvUvAcc], out: Collector[PvUvCount]): Unit = {
      val pvUvAcc = input.head
      val cts = System.currentTimeMillis()
      //println(s"$key - ${pvUvAcc.sid}")
      out.collect(PvUvCount(key, pvUvAcc.pv, pvUvAcc.uids.size, pvUvAcc.ts, window.getEnd, cts))
    }
  }

  def main(args: Array[String]): Unit = {
    test01(args)
  }
}
