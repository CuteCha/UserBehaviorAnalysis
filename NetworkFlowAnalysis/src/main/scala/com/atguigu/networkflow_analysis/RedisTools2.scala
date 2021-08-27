package com.atguigu.networkflow_analysis

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig, JedisShardInfo, ShardedJedis, ShardedJedisPool}

import java.util


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

case class RedisTools3(hostPort: List[(String, Int)], timeout: Int) {
  @transient private var pool: ShardedJedisPool = _

  private def makePool(hostPort: List[(String, Int)], timeout: Int): Unit = {
    if (pool == null) {
      val config = new GenericObjectPoolConfig()
      config.setMaxTotal(1024) // 最大连接数
      config.setMaxIdle(100) // 最大空闲连接数
      config.setMinIdle(10)
      config.setTestOnBorrow(true) // 检查连接可用性, 确保获取的redis实例可用
      config.setTestOnReturn(false)
      config.setMaxWaitMillis(10000)

      val shards = new util.ArrayList[JedisShardInfo]()
      hostPort.foreach { case (host, port) => shards.add(new JedisShardInfo(host, port, timeout)) }

      pool = new ShardedJedisPool(config, shards)

      val hook = new Thread {
        override def run(): Unit = pool.destroy()
      }
      sys.addShutdownHook(hook.run())
    }
  }

  def getPool: ShardedJedisPool = {
    if (pool == null) {
      makePool(hostPort, timeout)
    }

    pool
  }

}


object DebugRedisTools2 {
  def testRedisTools2(args: Array[String]): Unit = {
    val jedis: Jedis = RedisTools2("localhost", 6379, 20000).getPool.getResource
    jedis.set("a", "127")
    println(jedis.get("a"))
  }

  def testRedisTools3(args: Array[String]): Unit = {
    val jedis: ShardedJedis = RedisTools3(List(("localhost", 6379)), 20000).getPool.getResource
    jedis.set("a", "127")
    println(jedis.get("a"))
  }

  def main(args: Array[String]): Unit = {
    testRedisTools2(args)
  }
}
