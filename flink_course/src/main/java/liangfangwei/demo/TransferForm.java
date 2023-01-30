package liangfangwei.demo;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Tuple;

/**
 * 求不同性别中好友列表中好友最多的人
 * maxBy 大于等于才完全替换整个状态值
 */
public class TransferForm {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.readFile(new TextInputFormat(null), "flink_course/data/transformation_input/userinfo.txt",FileProcessingMode.PROCESS_ONCE,1000L);
        //  json对象转化Java对象
        SingleOutputStreamOperator<Tuple4<Integer, String, String, Integer>> maxDataStream = source.map(x -> JSONObject.parseObject(x, UserInfo.class)).flatMap(new FlatMapFunction<UserInfo, Tuple4<Integer, String, String, Integer>>() {

            @Override
            public void flatMap(UserInfo userInfo, Collector<Tuple4<Integer, String, String, Integer>> collector) throws Exception {
                collector.collect(Tuple4.of(userInfo.getUid(), userInfo.getName(), userInfo.getGender(), userInfo.getFriends().size()));

            }
        }).keyBy(tuple4 -> tuple4.f2).maxBy(3);
        maxDataStream.print("friendMax");
        env.execute();

    }
}
