package xuwei.tech.batch.batchAPI

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

/**
  * Created by xuwei.tech on 2018/10/30.
  */
object BatchDemoOuterJoinScala {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val data1 = ListBuffer[Tuple2[Int,String]]()
    data1.append((1,"zs"))
    data1.append((2,"ls"))
    data1.append((3,"ww"))


    val data2 = ListBuffer[Tuple2[Int,String]]()
    data2.append((1,"beijing"))
    data2.append((2,"shanghai"))
    data2.append((4,"guangzhou"))

    val text1 = env.fromCollection(data1)
    val text2 = env.fromCollection(data2)

    text1.leftOuterJoin(text2).where(0).equalTo(0).apply((first,second)=>{
      if(second==null){
        (first._1,first._2,"null")
      }else{
        (first._1,first._2,second._2)
      }
    }).print()

    println("===============================")

    text1.rightOuterJoin(text2).where(0).equalTo(0).apply((first,second)=>{
      if(first==null){
        (second._1,"null",second._2)
      }else{
        (first._1,first._2,second._2)
      }
    }).print()


    println("===============================")

    text1.fullOuterJoin(text2).where(0).equalTo(0).apply((first,second)=>{
      if(first==null){
        (second._1,"null",second._2)
      }else if(second==null){
        (first._1,first._2,"null")
      }else{
        (first._1,first._2,second._2)
      }
    }).print()




  }

}
