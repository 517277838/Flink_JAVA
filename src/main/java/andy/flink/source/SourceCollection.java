package andy.flink.source;

import org.apache.flink.api.common.RuntimeExecutionMode;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class SourceCollection {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 2.获取数据源
        /**
         常用于构造测试数据的api
        env.fromElements("可变参数")
        env.fromCollection("各种集合")
        env.generateSequence("定义一段整数的区间")
        env.fromSequence("定义一段整数的区间")
        **/

        DataStream<String> elements = env.fromElements("you", "are", "my", "destiny");

        DataStream<String> collection = env.fromCollection(new ArrayList<String>() {
            {
                add("you");
                add("are");
                add("my");
                add("destiny");
            }
        });

        DataStream<Long> genSqe = env.generateSequence(0L, 100L);

        DataStream<Long> sequence = env.fromSequence(0L, 100L);



        //TODO 3.数据转换，实现业务

        //TODO 4.将处理好的结果写入到指定的skin
        elements.printToErr("elements");
        collection.printToErr("collection");
        genSqe.printToErr("genseq");
        sequence.printToErr("sequence");

        env.execute("常用于测试，创建的数据源");

    }
}
