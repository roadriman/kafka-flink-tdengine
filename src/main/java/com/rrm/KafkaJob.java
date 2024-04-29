package com.rrm;

import com.rrm.bean.TdengineData;
import com.rrm.serial.CustomKafkaDeserializationSchema;
import com.rrm.utils.PropertiesUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class KafkaJob {

    final static Logger logger = LoggerFactory.getLogger(KafkaJob.class);

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 尝试重启的次数
                Time.of(10, TimeUnit.SECONDS) // 间隔
        ));
        KafkaSource<TdengineData> kafkaSource = KafkaSource.<TdengineData>builder()
                .setBootstrapServers( PropertiesUtils.readProperty("kafka.host"))
                .setTopics("td_w_test_test01")
                .setGroupId(PropertiesUtils.readProperty("kafka.groupid"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new CustomKafkaDeserializationSchema())
//                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        //流式处理，并行度最好与kafka partition相同
        DataStreamSource<TdengineData> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source").setParallelism(6);
        //测试用
        // DataStream<String> transction = env.fromCollection(Arrays.asList(
//                readTxt(PropertiesUtils.readProperty("test.file"))
//        ));
        stream.addSink(new Sink2TDengine()).setParallelism(6);
        env.execute();
    }

    private static String readTxt(String fileName) {
        StringBuffer str = new StringBuffer();
        try {
            List<String> lines = Files.readAllLines(Paths.get(fileName));
            for (String line : lines) {
                str.append(line);
            }
        } catch (IOException e) {
            logger.error("读取kafka数据出错！", e);
        }
        String insertStr = ",\"topicName\": \"td_w_test_testsupertable\"\n}";
        str.deleteCharAt(str.length() - 1).append(insertStr);
        return str.toString();
    }

}