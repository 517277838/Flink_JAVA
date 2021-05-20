package andy.flink.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MultiplePartition {
    public static void main(String[] args) throws Exception{
        //TODO 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //获取0~100的数字
        DataStreamSource<String> wcDS = env.readTextFile("/Users/lichao/Documents/work/develop/workspace/javaProject/Flink_JAVA/src/main/java/andy/flink/data/dspid.txt");


        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = wcDS.map(new RichMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                int taskID = getRuntimeContext().getIndexOfThisSubtask();
                return Tuple2.of(String.valueOf(taskID), 1);
            }
        }).keyBy(0).sum(1);

        resultStream.printToErr("no process");
        resultStream.rebalance().printToErr("rebalance");
        resultStream.global().printToErr("global");

        resultStream.forward().printToErr("forward");
        resultStream.rescale().printToErr("rescale");
        resultStream.shuffle().printToErr("shuffle");

        env.execute("MultiplePartition");


    }
}
