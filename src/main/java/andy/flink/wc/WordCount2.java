package andy.flink.wc;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;


/**
 * Desc 演示Flink-DataStream-API-实现WordCount
 * 注意:在Flink1.12中DataStream既支持流处理也支持批处理,如何区分?
 */

public class WordCount2 {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建环境

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         *  STREAMING,流
         *  BATCH,批
         *  AUTOMATIC;自动判断数据源是流还是批
         */
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //设置并发度，测试给1 生成上不适合做，有配置文件设置
        env.setParallelism(1);


        //TODO 2.获取数据源
       // DataStream<String> lines = env.fromElements("itcast hadoop spark", "itcast hadoop spark", "itcast hadoop", "itcast");


        DataStream<String> lines = env.fromCollection(new ArrayList<String>() {
            {
                add("hello");
                add("hello");
                add("hello");
                add("hello");
            }
        });


        //TODO 3.数据转换，实现业务
        DataStream<String> flatDS = lines.flatMap(
                new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                //value表示每一行数据
                String[] arr = value.split(" ");
                for (String word : arr) {
                    out.collect(word);
                }
            }
        });

        DataStream<Tuple2<String, Integer>> result = flatDS.map(
                new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        }).keyBy(t ->t.f0).sum(1);


        //TODO 4.将处理好的结果写入到指定的skin
        result.print();

        env.execute("离线数据源");




    }
}
