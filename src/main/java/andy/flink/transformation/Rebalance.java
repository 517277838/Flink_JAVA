package andy.flink.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Rebalance {
    public static void main(String[] args) throws Exception{
        //TODO 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //获取0~100的数字
        DataStreamSource<Long> LongDS = env.fromSequence(0, 100);
        //下面的操作相当于将数据随机分配一下,有可能出现数据倾斜
        SingleOutputStreamOperator<Long> filterStream = LongDS.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 10;
            }
        });
         //不实用reblace,使用   RichMapFunction,可以通过获取运行上下文，来获取任务ID
        /**
        no rebalance:6> (0,13)
        no rebalance:8> (2,13)
        no rebalance:1> (4,12)
        no rebalance:6> (1,12)
        no rebalance:8> (3,2)
        no rebalance:8> (5,13)
        no rebalance:8> (7,12)
        no rebalance:2> (6,13)
         **/
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> resultStream = filterStream.map(new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Long value) throws Exception {

                int taskId = getRuntimeContext().getIndexOfThisSubtask();
                return Tuple2.of(taskId, 1);
            }
        }).keyBy(0).sum(1);

        //调用reblace查看数据分布
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> resultStream2 = filterStream.rebalance().map(new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Long value) throws Exception {
                int taskId = getRuntimeContext().getIndexOfThisSubtask();

                return Tuple2.of(taskId, 1);
            }
        }).keyBy(new KeySelector<Tuple2<Integer, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<Integer, Integer> value) throws Exception {
                return value.f0;
            }
        }).sum(1);


        resultStream.printToErr("no rebalance");
        System.out.println("------------------------------");
        resultStream2.printToErr("rebalance ");


        env.execute("rebalance");
    }
}
