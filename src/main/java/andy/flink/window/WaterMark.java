package andy.flink.window;

import andy.flink.beans.SensorReading;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;

//为事件窗口，设置watermark
public class WaterMark {
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

        //设置事件事件
        SerializableTimestampAssigner<SensorReading> timestampAssigner = new SerializableTimestampAssigner<SensorReading>() {
            @Override
            public long extractTimestamp(SensorReading element, long recordTimestamp) {

                Long aLong = element.getTimestamp();
                return aLong * 1000L;
            }
        };

        SingleOutputStreamOperator<SensorReading> watermarkStream = sensorStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(timestampAssigner));

        SingleOutputStreamOperator<Tuple2<String, Integer>> waterStream = watermarkStream.flatMap(new FlatMapFunction<SensorReading, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(SensorReading value, Collector<Tuple2<String, Integer>> out) throws Exception {

                out.collect(new Tuple2<>(value.getId(), 1));
            }
        })
    .keyBy(data -> data.f0)
    .window(TumblingEventTimeWindows.of(Time.seconds(15)))//窗口大小
    .sum(1);


        waterStream.printToErr("waterStream");


        env.execute();
    }
}
