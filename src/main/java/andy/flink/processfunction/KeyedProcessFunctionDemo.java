package andy.flink.processfunction;

import andy.flink.beans.SensorReading;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

//定时任务
public class KeyedProcessFunctionDemo {
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
        // 测试KeyedProcessFunction，先分组然后自定义处理
        sensorStream.keyBy(new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading value) throws Exception {
                return value.getId();
            }
        })
        .process(new MyProcess())
        .printToErr("work");


        env.execute();
    }
    public static class MyProcess extends KeyedProcessFunction<String,SensorReading,Integer>{

        ValueState<Long> tsTimerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            tsTimerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-timer", Long.class));

        }

        //每来一条执行一次
        @Override
        public void processElement(SensorReading value, Context ctx, Collector<Integer> out) throws Exception {
            out.collect(value.getId().length());
            System.out.println(ctx.timerService().currentProcessingTime());
            //注册定时器
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime()+ 5000L);
            tsTimerState.update(ctx.timerService().currentProcessingTime() + 1000L);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println(timestamp+" 定时器触发 ");
        }

        @Override
        public void close() throws Exception {
            tsTimerState.clear();
        }

    }
}
