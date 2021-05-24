package andy.flink.window;

import andy.flink.beans.SensorReading;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
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

public class WaterMark02 {
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

        SingleOutputStreamOperator<SensorReading> resultStream = sensorStream.assignTimestampsAndWatermarks(new WatermarkStrategy<SensorReading>() {
            @Override
            public WatermarkGenerator<SensorReading> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<SensorReading>() {
                    private long maxTimesStamp = Long.MIN_VALUE;

                    // 每来一条数据，将这条数据与maxTimesStamp比较，看是否需要更新watermark
                    @Override
                    public void onEvent(SensorReading event, long eventTimestamp, WatermarkOutput output) {
                        maxTimesStamp = Math.max(event.getTimestamp(), maxTimesStamp);

                    }

                    // 周期性更新watermark
                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        // 允许乱序数据的最大限度为3s
                        long maxOutOfOrderness = 3000L;
                        output.emitWatermark(new Watermark(maxTimesStamp - maxOutOfOrderness));

                    }
                };
            }
        }
                //设置事件事件
                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp()* 1000L))
                .keyBy("id")
                // 创建长度为15s的事件时间窗口

                .window(TumblingEventTimeWindows.of(Time.seconds(15)))//窗口大小
                .apply(new WindowFunction<SensorReading, SensorReading, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<SensorReading> input, Collector<SensorReading> out) throws Exception {
                        System.out.println("window: [ " + window.getStart() + " - " + window.getEnd() + "]");
                        ArrayList<SensorReading> list = new ArrayList<>((Collection<? extends SensorReading>) input);

                        list.forEach(out::collect);

                    }
                });


        resultStream.printToErr("waterStream");


        env.execute();
    }
}
