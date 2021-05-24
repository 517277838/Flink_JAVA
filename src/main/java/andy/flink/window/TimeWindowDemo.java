package andy.flink.window;

import andy.flink.beans.SensorReading;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.scala.OutputTag;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class TimeWindowDemo extends Window {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 2.获取数据源

        //TODO 2.获取sock数据源

        DataStream<String> socketDS = env.socketTextStream("localhost", 7777);


        //封装为对象流
        DataStream<SensorReading> SensorStream = socketDS.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String id = value.split(",")[0];
                String timestamp = value.split(",")[1];
                String temperature = value.split(",")[2];
                return new SensorReading(id, Long.valueOf(timestamp), Double.valueOf(temperature));
            }
        });


        // 1. 增量聚合函数
        DataStream<Integer> resultStream = SensorStream.keyBy("id")
//                .countWindow(10, 2);
//                .window(EventTimeSessionWindows.withGap(Time.minutes(1)));
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
               .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))//1.12后版本的调用方式
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });

        resultStream.printToErr("local windown process !!!");

        //开一个15秒窗口，如果超过15秒，在等1分钟，如果超过1分钟的延迟设置，就输入到侧输出流里
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late", TypeInformation.of(SensorReading.class)) {
        };

        SingleOutputStreamOperator<SensorReading> lateStream = SensorStream.keyBy("id")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))//1.12后版本的调用方式
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .sum("temperature");


        lateStream.printToErr("late window process!!!");

        env.execute();
    }

    @Override
    public long maxTimestamp() {
        return 0;
    }
}
