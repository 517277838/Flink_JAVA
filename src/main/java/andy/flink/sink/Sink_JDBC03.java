package andy.flink.sink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Sink_JDBC03 {
    public static void main(String[] args) throws Exception {

        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();



        // sql语句，用问号做占位符
//        String sql = "insert into tb_traffic_statistic_min(starttime, city_name, distinct_user_count, total_traffic) values(?, ?, ?, ?)";
        String sql = "insert into sensor(id, times, temperature) values(?, ?, ?)";
        // 伪造数据
        Tuple3<String, String,  Double> bjTp = Tuple3.of("sensor_100", "1547718211",12.3d);

        env
                .fromElements(bjTp)
//                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.DOUBLE))
                // 添加JDBCSink
                .addSink(
                        JdbcSink.sink(
                                sql, // sql语句
                                // 设置占位符对应的字段值
                                (ps, tp) -> {
                                    ps.setString(1, tp.f0);
                                    ps.setString(2, tp.f1);
                                    ps.setDouble(3, tp.f2);

                                },
                                // 传递jdbc的连接属性
                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                        .withDriverName("com.mysql.jdbc.Driver")
                                        .withUrl("jdbc:mysql://192.168.0.33:3306/study")
                                        .withUsername("root")
                                        .withPassword("andy520")
                                        .build()
                        )
                );

        // 执行
        env.execute();
    }

}
