package andy.flink.state;

import andy.flink.beans.SensorReading;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class KeyedStateApplicationCase {
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

        // 定义一个flatmap操作，检测温度跳变，输出报警,flatmap 可以直接选择输出或不输出，不需要返回值
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> warmStream = sensorStream.keyBy(new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading value) throws Exception {
                return value.getId();
            }
        }).flatMap(new TempChangeWarning(10.0));

        warmStream.printToErr("warmStream");

        env.execute();
    }


        public static class TempChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>>{
            // 私有属性，温度跳变阈值,接收传参
            private Double threshold;

            public TempChangeWarning(Double threshold){
                this.threshold = threshold;
            }

            // 定义状态，保存上一次的温度值
            private ValueState<Double> lastTempState;


            @Override
            public void open(Configuration parameters) throws Exception {

                 //初始化状态
                lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class,Double.MIN_VALUE));
            }

            @Override
            public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {

                //获取状态
                Double lastTemp = lastTempState.value();
                if(lastTemp!=Double.MIN_VALUE) {
                    double diff = Math.abs(value.getTemperature() - lastTemp);
                    if (diff > threshold) {
                        out.collect(Tuple3.of(value.getId(), value.getTemperature(), lastTemp));
                    }
                }
                //跟新状态
                lastTempState.update(value.getTemperature());

            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        }

}
