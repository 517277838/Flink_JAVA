package andy.flink.state;

import andy.flink.beans.SensorReading;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateDemo01_KeyState {
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
        //实际中使用maxBy即可
        SingleOutputStreamOperator<SensorReading> maxbyStream = sensorStream.keyBy(new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading sensorReading) throws Exception {
                return sensorReading.getId();
            }
        }).maxBy("timestamp");

        maxbyStream.printToErr("max by");

        //学习时可以使用KeyState中的ValueState来实现maxBy的底层
        sensorStream.keyBy(new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading sensorReading) throws Exception {
                return sensorReading.getId();
            }
        }).map(new RichMapFunction<SensorReading, Tuple2<String,Integer>>() {
           //声明状态
            //-1.定义一个状态用来存放最大值
            private ValueState<Long> maxValueState;

            @Override
            public void open(Configuration parameters) throws Exception {
               //在open方法里初始化状态
                ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("maxValueState", Long.class);
                //根据状态描述器获取/初始化状态
                maxValueState = getRuntimeContext().getState(stateDescriptor);

            }

            @Override
            public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {
                    Long currentValue = sensorReading.getTimestamp();
                //获取状态
                   Long historyValue = maxValueState.value();


                return null;
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        });


        env.execute();
    }

}
