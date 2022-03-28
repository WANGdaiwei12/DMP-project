package com.wcw.util

import java.lang
import java.util.Properties

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.exceptions.JedisConnectionException
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * RedisPool工具类
 *
 * @description:${description}
 * @authror: snaker
 * @date: create:2022/3/27 下午 8:37
 * @version: V1.0
 * @modified By:
 */
object RedisPoolUtil {

  var jedisPool:JedisPool  = null;
 var redisConfigFile:String  = "redis.properties"
  //把redis连接对象放到本地线程中
var  local:ThreadLocal[Jedis] = new ThreadLocal[Jedis]()

  /**
   * 初始化连接池
   * @author corleone
   * @date 2018年11月27日
   */
  def initialPool()= {
    try {
    val props = new Properties();
    //加载连接池配置文件

    props.load(RedisPoolUtil.getClass.getClassLoader().getResourceAsStream(redisConfigFile));
    // 创建jedis池配置实例
    val config = new JedisPoolConfig();
    // 设置池配置项值
    config.setMaxTotal(Integer.valueOf(props.getProperty("pool.maxTotal")));
    config.setMaxIdle(Integer.valueOf(props.getProperty("pool.maxIdle")));

    config.setMaxWaitMillis(java.lang.Long.valueOf(props.getProperty("pool.maxWaitMillis")));
    var passWord: String = ""
    if ("".equals(props.getProperty("redis.passWord")))
      passWord
    else {
      passWord = props.getProperty("redis.passWord")
    }
    // 根据配置实例化jedis池
    jedisPool = new JedisPool(config, props.getProperty("redis.ip"),
      Integer.valueOf(props.getProperty("redis.port")),
      Integer.valueOf(props.getProperty("redis.timeout")), passWord
    )
    println("连接池初始化成功");
    }catch {
      case e:Exception=>{
        e.printStackTrace();
        System.err.println("连接池初始化失败");
      }
    }
  }

  /**
   * 获取连接
   * @return
   * @author corleone
   * @date 2018年11月27日
   */
  def getInstance():Jedis=  {
    //Redis对象
    var jedis:Jedis  =local.get();
    if(jedis==null){
      if (jedisPool == null) {
//        scala的同步代码块
       RedisPoolUtil.synchronized {
          if (jedisPool == null) {
            initialPool();
          }
        }
      }
      try{
        jedis = jedisPool.getResource();
      }catch {
        case e:JedisConnectionException=>{
          e.printStackTrace();
          }
        }
      local.set(jedis);
    }
     jedis
  }

  /**
   * 关闭连接
   * @author corleone
   * @date 2018年11月27日
   */
  def closeConn() {
    var jedis:Jedis  = local.get();
    if (jedis != null) {
      jedis.close();
    }
    local.set(null);
  }
}

class RedisPoolUtil{}
