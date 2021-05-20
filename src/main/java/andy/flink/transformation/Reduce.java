package andy.flink.transformation;

import andy.flink.beans.SensorReading;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Reduce {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 2.获取数据源

        //TODO 2.获取sock数据源

        DataStream<String> socketDS = env.socketTextStream("localhost", 7777);

        DataStream<SensorReading> SensorStream = socketDS.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String id = value.split(",")[0];
                String timestamp = value.split(",")[1];
                String temperature = value.split(",")[2];


                return new SensorReading(id, Long.valueOf(timestamp), Double.valueOf(temperature));
            }
        });


//        KeyedStream<SensorReading, Tuple> keybyStream = SensorStream.keyBy("id");

        KeyedStream<SensorReading, String> keybyStream = SensorStream.keyBy(new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading value) throws Exception {
                return value.getId();
            }
        });


        // reduce聚合，取最大的温度值，以及当前最新的时间戳
        SingleOutputStreamOperator<SensorReading> reduceStream = keybyStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                System.out.println("last: "+value1.toString());
                System.out.println("current: "+value2.toString());

                return new SensorReading(value1.getId(), value2.getTimestamp(), Math.max(value1.getTemperature(), value2.getTemperature()));
            }
        });

        reduceStream.printToErr("reduce");

        env.execute();
    }
}
