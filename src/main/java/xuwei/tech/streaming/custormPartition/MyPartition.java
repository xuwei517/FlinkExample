package xuwei.tech.streaming.custormPartition;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * Created by xuwei.tech on 2018/10/23.
 */
public class MyPartition implements Partitioner<Long> {
    @Override
    public int partition(Long key, int numPartitions) {
        System.out.println("分区总数："+numPartitions);
        if(key % 2 == 0){
            return 0;
        }else{
            return 1;
        }
    }
}
