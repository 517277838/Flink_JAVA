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

public class ApplicationCase {
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
        //需求：实现自定义处理函数，检测一段时间内(10s)的温度连续上升，输出报警
        //先分组
        sensorStream.keyBy(new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading value) throws Exception {
                return value.getId();
            }
        })
        .process(new TempConsIncreWarning(10))
                .printToErr("TempConsIncreWarning");

        env.execute();
    }

    public static class TempConsIncreWarning extends KeyedProcessFunction<String,SensorReading,String>{

        private Integer timerange;
        public TempConsIncreWarning(Integer timerange){
            this.timerange = timerange;
        }

        //定义状态
        ValueState<Double> lastTemp;
        ValueState<Long> tsTimerState;
        @Override
        public void open(Configuration parameters) throws Exception {
            //设置默认大小，为double的最小值
            lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class,Double.MIN_VALUE));
            tsTimerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-timer", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            Double historyTemp = lastTemp.value();
            Long tsTimer = tsTimerState.value();
            Double currentTemp = value.getTemperature();
            // 如果温度上升并且没有定时器，注册10秒后的定时器，开始等待
            if(historyTemp!=Double.MIN_VALUE ){
                System.out.println("historyTemp: "+historyTemp+"\t"+"currentTemp: "+currentTemp);
                if(currentTemp > historyTemp && tsTimer ==null){
                    // 计算出定时器时间戳,使用系统时间
                    Long ts = ctx.timerService().currentProcessingTime() + timerange * 1000L;
                    // 注册定时器
                    ctx.timerService().registerProcessingTimeTimer(ts);
                    //更新定时状态
                    tsTimerState.update(ts);

                    //温度没有连续上升，则删除定时器
                }else if(currentTemp < historyTemp && tsTimer !=null){
                    System.out.println("温度不连续上升！！！"+"\t"+"当前温度： "+currentTemp+"\t"+"上一次温度值："+historyTemp);
                    ctx.timerService().deleteProcessingTimeTimer(tsTimer);
                    tsTimerState.clear();
                }
            }else {
                System.out.println("第一次接收数据");
            }

            System.out.println(tsTimerState.value());

            //跟新温度状态
            lastTemp.update(currentTemp);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，输出报警信息
            out.collect("  传感器： "+ctx.getCurrentKey()+"  温度值连续 在" + timerange+" s 内上升"+"\t"+
                    "上一次温度值: "+lastTemp.value());

            tsTimerState.clear();

        }



        @Override
        public void close() throws Exception {
           lastTemp.clear();

        }
    }
}
