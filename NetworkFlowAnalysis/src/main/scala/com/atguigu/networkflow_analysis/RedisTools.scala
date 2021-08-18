package com.atguigu.networkflow_analysis

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object RedisTools extends Serializable {
  @transient private var pool: JedisPool = null

  def initJedis: Unit = {
    val redisHost = "localhost"
    val redisPort = 6379
    val redisTimeout = 20000

    makePool(redisHost, redisPort, redisTimeout)
  }

  def makePool(redisHost: String, redisPort: Int, redisTimeout: Int): Unit = {

    init(redisHost, redisPort, redisTimeout)
  }

  def init(redisHost: String, redisPort: Int, redisTimeout: Int): Unit = {
    if (pool == null) {
      val poolConfig = new GenericObjectPoolConfig()
      poolConfig.setMaxTotal(1024)  // 最大连接数
      poolConfig.setMaxIdle(100) // 最大空闲连接数
      poolConfig.setMinIdle(10)
      poolConfig.setTestOnBorrow(true) // 检查连接可用性, 确保获取的redis实例可用
      poolConfig.setTestOnReturn(false)
      poolConfig.setMaxWaitMillis(10000)

      pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout)

      val hook = new Thread {
        override def run = pool.destroy()
      }
      sys.addShutdownHook(hook.run)
    }
  }

  def getPool: JedisPool = {
    if (pool == null) {
      initJedis
    }

    pool
  }

  def test(args: Array[String]): Unit = {
    val jedis = getPool.getResource
    jedis.set("xxx", "a2")

  }

  def main(args: Array[String]): Unit = {
    test(args)
  }
}
