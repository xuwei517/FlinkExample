package xuwei.tech.streaming.sink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  *
  *
  * Created by xuwei.tech on 2018/10/8.
  */
object StreamingDataToRedisScala {

  def main(args: Array[String]): Unit = {

    //获取socket端口号
    val port = 9000

    //获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //链接socket获取输入数据
    val text = env.socketTextStream("hadoop100",port,'\n')

    //注意：必须要添加这一行隐式转行，否则下面的flatmap方法执行会报错
    import org.apache.flink.api.scala._

    val l_wordsData = text.map(line=>("l_words_scala",line))

    val conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop110").setPort(6379).build()

    val redisSink = new RedisSink[Tuple2[String,String]](conf,new MyRedisMapper)

    l_wordsData.addSink(redisSink)

    //执行任务
    env.execute("Socket window count");


  }

  class MyRedisMapper extends RedisMapper[Tuple2[String,String]]{

    override def getKeyFromData(data: (String, String)) = {
      data._1
    }

    override def getValueFromData(data: (String, String)) = {
      data._2
    }

    override def getCommandDescription = {
      new RedisCommandDescription(RedisCommand.LPUSH)
    }
  }


}
