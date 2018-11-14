package xuwei.tech.streaming

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Created by xuwei.tech on 2018/10/23.
  */
object StreamingFromCollectionScala {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //隐式转换
    import org.apache.flink.api.scala._

    val data = List(10,15,20)

    val text = env.fromCollection(data)

    //针对map接收到的数据执行加1的操作
    val num = text.map(_+1)

    num.print().setParallelism(1)

    env.execute("StreamingFromCollectionScala")



  }

}
