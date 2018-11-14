package xuwei.tech.batch.batchAPI

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.ListBuffer

/**
  * broadcast 广播变量
  * Created by xuwei.tech on 2018/10/30.
  */
object BatchDemoBroadcastScala {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    //1: 准备需要广播的数据
    val broadData = ListBuffer[Tuple2[String,Int]]()
    broadData.append(("zs",18))
    broadData.append(("ls",20))
    broadData.append(("ww",17))

    //1.1处理需要广播的数据
    val tupleData = env.fromCollection(broadData)
    val toBroadcastData = tupleData.map(tup=>{
      Map(tup._1->tup._2)
    })


    val text = env.fromElements("zs","ls","ww")

    val result = text.map(new RichMapFunction[String,String] {


      var listData: java.util.List[Map[String,Int]] = null
      var allMap  = Map[String,Int]()

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        this.listData = getRuntimeContext.getBroadcastVariable[Map[String,Int]]("broadcastMapName")
        val it = listData.iterator()
        while (it.hasNext){
          val next = it.next()
          allMap = allMap.++(next)
        }
      }

      override def map(value: String) = {
        val age = allMap.get(value).get
        value+","+age
      }
    }).withBroadcastSet(toBroadcastData,"broadcastMapName")


  result.print()

  }

}
