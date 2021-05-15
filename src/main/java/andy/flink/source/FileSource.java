package andy.flink.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileSource {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 2.获取数据源
        //获取文本数据
        DataStream<String> input = env.readTextFile("/Users/lichao/Documents/work/develop/workspace/javaProject/Flink_JAVA/src/main/java/andy/flink/data/input")
                .filter(x->x.length()!=0);
        //获取文件夹的数据
        DataStream<String> dir = env.readTextFile("/Users/lichao/Documents/work/develop/workspace/javaProject/Flink_JAVA/src/main/java/andy/flink/data")
                .filter(x->x.length()!=0);;

        DataStream<String> gzDS = env.readTextFile("/Users/lichao/Documents/work/develop/workspace/javaProject/Flink_JAVA/src/main/java/andy/flink/data/input1.tar.gz");


        //TODO 3.数据转换，实现业务


        //TODO 4.将处理好的结果写入到指定的skin
//        input.printToErr("文本数据");
//        dir.printToErr("文件夹数据");
        gzDS.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {

                return !value.isEmpty();
            }
        }).printToErr("压缩文件");

        //TODO 5. 启动flink流程序
        env.execute("读取磁盘文件、文件夹、压缩包");

    }
}
