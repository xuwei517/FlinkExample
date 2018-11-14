package xuwei.tech.batch.batchAPI

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

/**
  * Created by xuwei.tech on 2018/10/30.
  */
object BatchDemoFirstNScala {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val data = ListBuffer[Tuple2[Int,String]]()
    data.append((2,"zs"))
    data.append((4,"ls"))
    data.append((3,"ww"))
    data.append((1,"xw"))
    data.append((1,"aw"))
    data.append((1,"mw"))

    val text = env.fromCollection(data)

    //获取前3条数据，按照数据插入的顺序
    text.first(3).print()
    println("==============================")

    //根据数据中的第一列进行分组，获取每组的前2个元素
    text.groupBy(0).first(2).print()
    println("==============================")


    //根据数据中的第一列分组，再根据第二列进行组内排序[升序]，获取每组的前2个元素
    text.groupBy(0).sortGroup(1,Order.ASCENDING).first(2).print()
    println("==============================")


    //不分组，全局排序获取集合中的前3个元素，
    text.sortPartition(0,Order.ASCENDING).sortPartition(1,Order.DESCENDING).first(3).print()





  }

}
