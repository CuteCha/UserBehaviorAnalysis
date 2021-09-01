package com.atguigu.test

object Debug {
  case class ActionACC(pv: Long, uv: Long, uidSet: Set[String])

  def test01(args: Array[String]): Unit = {
    val mAgg = Map(
      "a" -> ActionACC(10, 2, Set[String]("u1", "u3")),
      "b" -> ActionACC(10, 2, Set[String]("u2", "u3"))
    )
    val mAcc = Map("a" -> ActionACC(10, 2, Set[String]("u1", "u2")))

    val agg = mAgg.map(x => {
      val v = if (mAcc.contains(x._1)) mAcc.getOrElse(x._1, x._2) else x._2
      x._1 -> v
    })

    agg.foreach(x => println(s"${x._1},${x._2.pv},${x._2.uv},${x._2.uidSet.toList.mkString("|")}"))

  }

  def main(args: Array[String]): Unit = {
    test01(args)
  }
}
