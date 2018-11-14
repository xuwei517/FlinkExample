package xuwei.tech.streaming.custormSource

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

/**
  * 创建自定义并行度为1的source
  *
  * 实现从1开始产生递增数字
  *
  * Created by xuwei.tech on 2018/10/23.
  */
class MyRichParallelSourceScala extends RichParallelSourceFunction[Long]{

  var count = 1L
  var isRunning = true

  override def run(ctx: SourceContext[Long]) = {
    while(isRunning){
      ctx.collect(count)
      count+=1
      Thread.sleep(1000)
    }

  }

  override def cancel() = {
    isRunning = false
  }

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def close(): Unit = super.close()
}
