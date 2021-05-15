package andy.flink.wc;

import org.apache.flink.api.common.RuntimeExecutionMode;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * Desc 演示Flink-DataStream-API-实现WordCount
 * DataStream-匿名内部类-处理流socket
 */

public class WordCount3 {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建环境

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /**
         *  STREAMING,流
         *  BATCH,批
         *  AUTOMATIC;自动判断数据源是流还是批
         */
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //设置并发度，测试给1 生成上不适合做，有配置文件设置
        env.setParallelism(1);


        //TODO 2.获取数据源 socket
//        DataStream<String> lines = env.fromElements("itcast hadoop spark", "itcast hadoop spark", "itcast hadoop", "itcast");

        DataStream<String> lines = env.socketTextStream("192.168.0.116", 7777);


        //TODO 3.数据转换，实现业务
        //使用匿名类
//        DataStream <Tuple2<String, Integer>> result = lines.flatMap(
//                new FlatMapFunction<String, Tuple2<String, Integer>>() {
//                    @Override
//                    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
//                        for (String word : line.split(" ")) {
//                            out.collect(Tuple2.of(word, 1));
//                        }
//
//                    }
//                }).keyBy(t -> t.f0).sum(1);

        //实现自己实现
        DataStream<Tuple2<String, Integer>> resultStream = lines.flatMap(new MyFlatMap()).keyBy(t -> t.f0).sum(1);


        //TODO 4.将处理好的结果写入到指定的skin
        resultStream.print();

        env.execute("离线数据源");




    }

    public static class MyFlatMap extends RichFlatMapFunction<String,Tuple2<String,Integer>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for (String word : value.split(" ")) {

                collector.collect(Tuple2.of(word, 1));

            }
        }

        @Override
        public void close() throws Exception {
            System.out.println("结束执行，只执行一次");
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("初始化执行，只执行一次");
        }
    }
}
