package andy.flink.state;

import andy.flink.beans.SensorReading;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

public class OperatorState {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 2.获取数据源

        //TODO 2.获取sock数据源

        DataStream<String> socketDS = env.socketTextStream("localhost", 7777);


        //封装为对象流
        DataStream<SensorReading> sensorStream = socketDS.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String id = value.split(",")[0];
                String timestamp = value.split(",")[1];
                String temperature = value.split(",")[2];


                return new SensorReading(id, Long.valueOf(timestamp), Double.valueOf(temperature));
            }
        });


        //TODO
        // 定义一个有状态的map操作，统计当前分区数据个数
        SingleOutputStreamOperator<Integer> countStream = sensorStream.map(new MyCountMapper());

        countStream.printToErr("countStream");



        env.execute();
    }
    public static class MyCountMapper implements MapFunction<SensorReading,Integer>, ListCheckpointed<Integer>{
        // 定义一个本地变量，作为算子状态
        private Integer count = 0;

        @Override
        public Integer map(SensorReading value) throws Exception {
            count++;
            return count;
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for (Integer num : state) {
                count += num;
            }

        }
    }
}
