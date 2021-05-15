package andy.flink.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SocketSource {
    public static void main(String[] args) throws Exception {

        //TODO 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);



        //TODO 2.获取数据源

        DataStream<String> socketDS = env.socketTextStream("localhost", 7777);


        //TODO 3.数据转换，实现业务 wc
        DataStream<Tuple2<String, Integer>> wcDS = socketDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String word : line.split(" ")) {
                    out.collect(Tuple2.of(word, 1));

                }

            }


        });

        DataStream<Tuple2<String, Integer>> result = wcDS.keyBy(t -> t.f0).sum(1);


        //TODO 4.将处理好的结果写入到指定的skin
        result.printToErr("socket wc ");


        //TODO 5. 启动flink流程序
        env.execute("获取socket传来的数据");

    }
}
