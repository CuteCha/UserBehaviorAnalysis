package com.atguigu.networkflow_analysis

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

case class RedisTools2(host: String, port: Int, timeout: Int) {
  @transient private var pool: JedisPool = _


  private def makePool(host: String, port: Int, timeout: Int): Unit = {
    if (pool == null) {
      val poolConfig = new GenericObjectPoolConfig()
      poolConfig.setMaxTotal(1024) // 最大连接数
      poolConfig.setMaxIdle(100) // 最大空闲连接数
      poolConfig.setMinIdle(10)
      poolConfig.setTestOnBorrow(true) // 检查连接可用性, 确保获取的redis实例可用
      poolConfig.setTestOnReturn(false)
      poolConfig.setMaxWaitMillis(10000)

      pool = new JedisPool(poolConfig, host, port, timeout)

      val hook = new Thread {
        override def run(): Unit = pool.destroy()
      }
      sys.addShutdownHook(hook.run())
    }
  }

  def getPool: JedisPool = {
    if (pool == null) {
      makePool(host, port, timeout)
    }

    pool
  }

}

object DebugRedisTools2 {
  def main(args: Array[String]): Unit = {
    val jedis = RedisTools2("localhost", 6379, 20000).getPool.getResource
    jedis.set("a", "127")
    println(jedis.get("a"))
  }
}
