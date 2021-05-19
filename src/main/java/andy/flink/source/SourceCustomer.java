package andy.flink.source;


/**
 * 随机生成数据
 * 用户模拟生成一些数据
 *
 *
 */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.UUID;

/**
 * 需求
 * 每隔一秒随机生成一条订单数据（订单ID，用户ID，订单金额，时间戳）
 */

public class SourceCustomer {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //TODO 2.获取数据源
        //    我们自己实现SourceFunction
        //    public interface SourceFunction<T> extends Function, Serializable {
        DataStream<Order> OrderDS = env.addSource(new MyCustomer());


        //TODO 3.数据转换，实现业务 wc


        //TODO 4.将处理好的结果写入到指定的skin
       //result.printToErr("socket wc ");
        OrderDS.printToErr("order info");


        //TODO 5. 启动flink流程序

        env.execute("随机生成订单信息");

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    // MyCustomer因为是静态，所以需要static修饰
    public static class Order{
        private String id;
        private Integer userId;
        private Integer money;
        private Long createTime;

    }
        //主方法为静态，所以要使用static修饰
    public static class MyCustomer extends RichParallelSourceFunction<Order>{
        Boolean flag = true;

        @Override
        public void run(SourceContext<Order> ctx) throws Exception {
            Random random = new Random();
            while (flag) {
                String oid = UUID.randomUUID().toString();
                int userId = random.nextInt(3);
                int money = random.nextInt(101);
                long createTime = System.currentTimeMillis();
                ctx.collect(new Order(oid,userId,money,createTime));
                //then money eq 100 change flag to flase
                if(money==100){
                    System.out.println("money is 100 exit");
                    flag = false;
                }
                Thread.sleep(1000);
            }

        }

        @Override
        public void cancel() {
            System.out.println("cancel the task");
            flag = false;

        }

            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("init only exe one times");
            }

            @Override
            public void close() throws Exception {
                System.out.println("exe then the task finsh");
            }
        }


}
