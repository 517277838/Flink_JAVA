package andy.flink.sink;

import andy.flink.beans.SensorReading;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
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

        DataStream<String> socketDS = env.socketTextStream("localhost", 7777);


        //封装为对象流
        DataStream<SensorReading> SensorStream = socketDS.map(new MapFunction<String, SensorReading>() {
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
                "insert into sensor (id, times,temperature) values (?, ?,?)",

        new MyJdbcStatementBuilder(), new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://192.168.0.33:3306/study")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("andy520").build()
        ));


        env.execute();
    }

    public static class MyJdbcStatementBuilder implements JdbcStatementBuilder<SensorReading> {

        @Override
        public void accept(PreparedStatement preparedStatement, SensorReading sensorReading) throws SQLException {
            preparedStatement.setString(1, sensorReading.getId());
            preparedStatement.setString(2, sensorReading.getTimestamp().toString());
            preparedStatement.setDouble(3, sensorReading.getTemperature());
            preparedStatement.execute();
        }
    }
}
