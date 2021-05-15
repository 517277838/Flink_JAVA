package andy.flink.wc;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hive.conf.Validator;


/**
 * Desc 演示Flink-DataStream-API-实现WordCount
 * DataStream-匿名内部类-处理流socket
 */
//Lambda写法

public class WordCount4 {
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


        //TODO 2.获取数据源 socket
       // DataStream<String> lines = env.fromElements("itcast hadoop spark", "itcast hadoop spark", "itcast hadoop", "itcast");

        DataStream<String> lines = env.socketTextStream("192.168.0.116", 7777);


        //TODO 3.数据转换，实现业务
        // 使用labmd方式，需要给指定类型
        DataStream <Tuple2<String, Integer>> result = lines.flatMap(
                (String value,Collector<Tuple2<String,Integer>> out ) -> {
                    for (String word : value.split(" ")) {
                        out.collect(Tuple2.of(word,1));
                    }
                }
        ).returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(k->k.f0).sum(1);


        //TODO 4.将处理好的结果写入到指定的skin
        result.print();

        env.execute("离线数据源");




    }
}
