package andy.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;


//Desc 演示Flink-DataSet-API-实现WordCount

public class WordCount {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //设置并发度，测试给1 生成上不适合做，有配置文件设置
        env.setParallelism(1);


        //TODO 2.获取数据源
        DataSet<String> lines = env.fromElements("itcast hadoop spark", "itcast hadoop spark", "itcast hadoop", "itcast");


        DataSet<String> list = env.fromCollection(new ArrayList<String>() {
            {
                add("hello");
                add("hello");
                add("hello");
                add("hello");
            }
        });



        //TODO 3.数据转换，实现业务
        FlatMapOperator<String, String> flatDS = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                //value表示每一行数据
                String[] arr = value.split(" ");
                for (String word : arr) {
                    out.collect(word);
                }
            }
        });

        AggregateOperator<Tuple2<String, Integer>> result = flatDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        }).groupBy(0).sum(1);


        //TODO 4.将处理好的结果写入到指定的skin
        result.print();




    }
}
