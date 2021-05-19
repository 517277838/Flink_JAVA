package andy.flink.source;

import andy.flink.beans.SensorReading;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SensorSource {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 2.获取数据源
        //获取文本数据
        DataStream<String> input = env.readTextFile("/Users/lichao/Documents/work/develop/workspace/javaProject/Flink_JAVA/src/main/java/andy/flink/data/sensor.txt");
        DataStream<SensorReading> SensorStream = input.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String id = value.split(",")[0];
                String timestamp = value.split(",")[1];
                String temperature = value.split(",")[2];


                return new SensorReading(id, Long.valueOf(timestamp), Double.valueOf(temperature));
            }
        });

        KeyedStream<SensorReading, Object> keyedStream = SensorStream.keyBy(new KeySelector<SensorReading, Object>() {
            @Override
            public Object getKey(SensorReading value) throws Exception {
                return value.getId();
            }
        });

        DataStream<Tuple2<String, Integer>> resultStream = keyedStream.map(new MapFunction<SensorReading, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(SensorReading value) throws Exception {
                return Tuple2.of(value.getId(), 1);
            }
        });

        keyedStream.print("bykey");

        resultStream.printToErr("result");

        SingleOutputStreamOperator<Tuple2<String, Integer>> wcStream = resultStream.keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).sum(1);








        wcStream.printToErr("wc");


        env.execute();
    }
}
