package xuwei.tech.streaming.streamAPI

import org.apache.flink.api.common.functions.Partitioner

/**
  * Created by xuwei.tech on 2018/10/23.
  */
class MyPartitionerScala extends Partitioner[Long]{

  override def partition(key: Long, numPartitions: Int) = {
    println("分区总数："+numPartitions)
    if(key % 2 ==0){
      0
    }else{
      1
    }

  }

}
