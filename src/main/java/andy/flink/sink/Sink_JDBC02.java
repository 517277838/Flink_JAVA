package andy.flink.sink;

import andy.flink.beans.SensorReading;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Sink_JDBC02 {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 2.获取数据源

        //TODO 2.获取sock数据源

        DataStream<String> sourceStream = env.socketTextStream("localhost", 7777);


       // DataStreamSource<String> sourceStream = env.readTextFile("/Users/lichao/Documents/work/develop/workspace/javaProject/Flink_JAVA/src/main/java/andy/flink/data/sensor.txt");

        String sql = "insert into sensor(id, times, temperature) values(?, ?, ?)";
        //封装为对象流
        DataStream<SensorReading> SensorStream = sourceStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String id = value.split(",")[0];
                String timestamp = value.split(",")[1];
                String temperature = value.split(",")[2];


                return new SensorReading(id, Long.valueOf(timestamp), Double.valueOf(temperature));
            }
        });


        //TODO

        SensorStream.addSink(JdbcSink.sink(
                sql,

        new MyJdbcStatementBuilder(), new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://192.168.0.33:3306/study")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("andy520").build()
        ));




        SensorStream.printToErr("SensorStream");
        /**
        SensorStream
                .addSink(JdbcSink.sink(
                sql,
                (ps, value) -> {
                    ps.setString(1, value.getId());
                    ps.setString(2, value.getTimestamp().toString());
                    ps.setDouble(3, value.getTemperature());
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://192.168.0.33:3306/study")
                        .withUsername("root")
                        .withPassword("andy520")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .build()));
         **/

        env.execute();
    }

    public static class MyJdbcStatementBuilder implements JdbcStatementBuilder<SensorReading> {

        @Override
        public void accept(PreparedStatement preparedStatement, SensorReading sensorReading) throws SQLException {
            preparedStatement.setString(1, sensorReading.getId());
            preparedStatement.setString(2, sensorReading.getTimestamp().toString());
            preparedStatement.setDouble(3, sensorReading.getTemperature());
        }
    }
}
