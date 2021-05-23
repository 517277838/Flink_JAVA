package andy.flink.sink;

import andy.flink.beans.SensorReading;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Sink_JDBC01 {
    public static void main(String[] args) throws Exception {
        //TODO  1.创建环境
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

        SensorStream.printToErr("SensorStream");

        //TODO  插入mysql

        SensorStream.addSink(new MyJDBC());


        env.execute();
    }

    public static class MyJDBC extends RichSinkFunction<SensorReading>{
        //初始化mysql连接
        // 声明连接和预编译语句
        Connection connection = null;
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;
        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://192.168.0.33:3306/study", "root", "andy520");
            insertStmt = connection.prepareStatement("insert into sensor (id, times,temperature) values (?, ?,?)");
            updateStmt = connection.prepareStatement("update sensor set times =? , temperature = ? where id = ?");

        }

        @Override
        public void close() throws Exception {
            if(!connection.isClosed()){
                connection.close();
            }
            if(!insertStmt.isClosed()){
                insertStmt.close();
            }
            if(!updateStmt.isClosed()){
                updateStmt.close();
        }


    }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {

            // 直接执行更新语句，如果没有更新那么就插入
            updateStmt.setString(1, value.getTimestamp().toString());
            updateStmt.setDouble(2, value.getTemperature());
            updateStmt.setString(3, value.getId());
            updateStmt.execute();
            if( updateStmt.getUpdateCount() == 0 ){
                insertStmt.setString(1, value.getId());
                insertStmt.setString(2, value.getTimestamp().toString());
                insertStmt.setDouble(3, value.getTemperature());
                insertStmt.execute();
            }

        }
    }
}
