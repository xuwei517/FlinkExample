package xuwei.tech;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Created by xuwei.tech on 2018/10/8.
 */
public class BatchWordCountJava {

    public static void main(String[] args) throws Exception{
        String inputPath = "D:\\data\\file";
        String outPath = "D:\\data\\result";

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //获取文件中的内容
        DataSource<String> text = env.readTextFile(inputPath);

        DataSet<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer()).groupBy(0).sum(1);
        counts.writeAsCsv(outPath,"\n"," ").setParallelism(1);
        env.execute("batch word count");

    }


    public static class Tokenizer implements FlatMapFunction<String,Tuple2<String,Integer>>{
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token: tokens) {
                if(token.length()>0){
                    out.collect(new Tuple2<String, Integer>(token,1));
                }
            }
        }
    }
}
