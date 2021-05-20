package andy.flink.transformation;

import andy.flink.beans.SensorReading;
import com.ibm.icu.util.Output;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;

public class MultipleStreams {

    //温度超过30度，定为高温，低于定为低温。定义标签
    static  OutputTag<SensorReading> hightTag = new OutputTag<>("hight", TypeInformation.of(SensorReading.class));
    static OutputTag<SensorReading> lowerTag = new OutputTag<>("lower", TypeInformation.of(SensorReading.class));

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

        //TODO  将一条流分为两条流
        // 以前的split方法已被启用，需要通过pocess方法，自定义来实现
        SingleOutputStreamOperator<SensorReading> resultStream = SensorStream.process(new Silde());
        //获取不同标签的流
        DataStream<SensorReading> hightStream = resultStream.getSideOutput(hightTag);
        DataStream<SensorReading> lowerStream = resultStream.getSideOutput(lowerTag);


        //合并流
        // 2. 合流 connect，将高温流转换成二元组类型，与低温流连接合并之后，输出状态信息
        SingleOutputStreamOperator<Tuple2<String, Double>> warmStream = hightStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return Tuple2.of(value.getId(), value.getTemperature());
            }
        });

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectStream = warmStream.connect(lowerStream);
        SingleOutputStreamOperator<Object> resultStream1 = connectStream.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return Tuple3.of(value.f0, value.f1, "hight");
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), "normal");
            }
        });
        resultStream1.printToErr("resultStream1");

        env.execute();
    }


    public static class Silde extends ProcessFunction<SensorReading,SensorReading>{



        @Override
        public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {

            //超过30度为高温
            if(value.getTemperature()>30){
                //定义一个高温侧输出流,并打上标签
                ctx.output(hightTag, value);
            }else {
                ctx.output(lowerTag, value);
            }
        }
    }
}
