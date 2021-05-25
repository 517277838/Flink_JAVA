package andy.flink.tableapi;

import andy.flink.beans.SensorReading;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

public class CommonApi {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 2.获取数据源

        //TODO 2.获取sock数据源

        DataStream<String> socketDS = env.socketTextStream("localhost", 7777);


        //封装为对象流
        DataStream<SensorReading> sensorStream = socketDS.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String id = value.split(",")[0];
                String timestamp = value.split(",")[1];
                String temperature = value.split(",")[2];


                return new SensorReading(id, Long.valueOf(timestamp), Double.valueOf(temperature));
            }
        });


        //TODO

        //创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String filePath = "/Users/lichao/Documents/work/develop/workspace/javaProject/Flink_JAVA/src/main/java/andy/flink/data/sensor.txt";
        tableEnv.connect( new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("timestamp", DataTypes.BIGINT())
                .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable");

        Table inputTable = tableEnv.from("inputTable");
        // 3. 查询转换
        // 3.1 Table API
        // 简单转换
        Table resultTable = inputTable.select("id, temp")
                .filter("id === 'sensor_6'");

        Table avgTable = inputTable.groupBy("id")
                .select("id,id.count as ct ");

        tableEnv.toAppendStream(resultTable, Row.class).printToErr("resultTable");
        tableEnv.toRetractStream(avgTable, Row.class).printToErr("avgTable");

        //输出文件
        String outPath = "/Users/lichao/Documents/work/develop/workspace/javaProject/Flink_JAVA/src/main/java/andy/flink/data/out.txt";
        tableEnv.connect(new FileSystem().path(outPath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("outputTable");


        resultTable.executeInsert("outputTable");
        env.execute();
    }
}
