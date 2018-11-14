package xuwei.tech.streaming

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper

/**
  * Created by xuwei.tech on 2018/10/23.
  */
object StreamingKafkaSinkScala {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //隐式转换
    import org.apache.flink.api.scala._


    //checkpoint配置
    env.enableCheckpointing(5000);
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500);
    env.getCheckpointConfig.setCheckpointTimeout(60000);
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1);
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

    //设置statebackend

    //env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop100:9000/flink/checkpoints",true));


    val text = env.socketTextStream("hadoop100",9001,'\n')


    val topic = "t1"
    val prop = new Properties()
    prop.setProperty("bootstrap.servers","hadoop110:9092")
    //第一种解决方案，设置FlinkKafkaProducer011里面的事务超时时间
    //设置事务超时时间
    //prop.setProperty("transaction.timeout.ms",60000*15+"");

    //第二种解决方案，设置kafka的最大事务超时时间

    //FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<>(brokerList, topic, new SimpleStringSchema());

    //使用支持仅一次语义的形式
    val myProducer = new FlinkKafkaProducer011[String](topic,new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()), prop, FlinkKafkaProducer011.Semantic.EXACTLY_ONCE)
    text.addSink(myProducer)

    env.execute("StreamingFromCollectionScala")



  }

}
