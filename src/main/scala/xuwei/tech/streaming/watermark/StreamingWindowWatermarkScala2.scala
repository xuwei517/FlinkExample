package xuwei.tech.streaming.watermark

import java.text.SimpleDateFormat

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting

/**
  * Watermark 案例
  *
  * sideOutputLateData 收集迟到的数据
  *
  * Created by xuwei.tech
  */
object StreamingWindowWatermarkScala2 {

  def main(args: Array[String]): Unit = {
    val port = 9000
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    val text = env.socketTextStream("hadoop100",port,'\n')

    val inputMap = text.map(line=>{
      val arr = line.split(",")
      (arr(0),arr(1).toLong)
    })

    val waterMarkStream = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {
      var currentMaxTimestamp = 0L
      var maxOutOfOrderness = 10000L// 最大允许的乱序时间是10s

      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

      override def getCurrentWatermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)

      override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long) = {
        val timestamp = element._2
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        val id = Thread.currentThread().getId
        println("currentThreadId:"+id+",key:"+element._1+",eventtime:["+element._2+"|"+sdf.format(element._2)+"],currentMaxTimestamp:["+currentMaxTimestamp+"|"+ sdf.format(currentMaxTimestamp)+"],watermark:["+getCurrentWatermark().getTimestamp+"|"+sdf.format(getCurrentWatermark().getTimestamp)+"]")
        timestamp
      }
    })

    val outputTag = new OutputTag[Tuple2[String,Long]]("late-data"){}

    val window = waterMarkStream.keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(3))) //按照消息的EventTime分配窗口，和调用TimeWindow效果一样
      //.allowedLateness(Time.seconds(2))//允许数据迟到2秒
      .sideOutputLateData(outputTag)
      .apply(new WindowFunction[Tuple2[String, Long], String, Tuple, TimeWindow] {
      override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[String]) = {
        val keyStr = key.toString
        val arrBuf = ArrayBuffer[Long]()
        val ite = input.iterator
        while (ite.hasNext){
          val tup2 = ite.next()
          arrBuf.append(tup2._2)
        }

        val arr = arrBuf.toArray
        Sorting.quickSort(arr)

        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        val result = keyStr + "," + arr.length + "," + sdf.format(arr.head) + "," + sdf.format(arr.last)+ "," + sdf.format(window.getStart) + "," + sdf.format(window.getEnd)
        out.collect(result)
      }
    })

    val sideOutput: DataStream[Tuple2[String, Long]] = window.getSideOutput(outputTag)

    sideOutput.print()

    window.print()

    env.execute("StreamingWindowWatermarkScala")

  }



}
