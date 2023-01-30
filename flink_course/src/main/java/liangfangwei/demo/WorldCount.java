package liangfangwei.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Collector;


/**
 * 聚合7777 端口处每个单词的数量
 */
public class WorldCount {
    public static void main(String[] args) throws Exception {
        // 流批一体的执行环境
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 开启web-ui
//        Configuration conf = new Configuration();
//        conf.setInteger("rest.port", 9999);
        // 开启本地WEB_UI
     //   StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
      //  env.setParallelism(1);
        //env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        //  String filePath = "./flink_course/data/wc/input/";
        // DataStreamSource<String> socketTextStream = env.readFile(new TextInputFormat(null),filePath, FileProcessingMode.PROCESS_CONTINUOUSLY,1000);
         DataStreamSource<String> socketTextStream = env.socketTextStream("1.117.169.69", 7777);
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String words, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // \s+ 相当于空格 回车 tab符
                String[] s = words.split("\\s+");
                for (String word : s) {
                    //  collector.collect(new Tuple2<>(word, 1));
                    collector.collect(Tuple2.of(word, 1));
                }

            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        }).sum("f1");

        resultStream.print("word_count").setParallelism(1);
        env.execute();

    }
}
