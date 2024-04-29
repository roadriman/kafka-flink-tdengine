package com.rrm.serial;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.rrm.bean.TdengineData;
import com.rrm.bean.TdengineTagData;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

public class CustomKafkaDeserializationSchema implements KafkaRecordDeserializationSchema<TdengineData> {
    private static  String encoding = "UTF8";

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<TdengineData> collector) throws IOException {
        TdengineData tdengineData = parse(JSONObject.parseObject( new String(consumerRecord.value())));
        tdengineData.setTopicName(consumerRecord.topic());
        tdengineData.setPartition(consumerRecord.partition());
        collector.collect(tdengineData);

    }
    @Override
    public TypeInformation<TdengineData> getProducedType() {
        return TypeInformation.of(new TypeHint<TdengineData>(){});
    }

    private static TdengineData parse(JSONObject data) {
        TdengineData tdengineData = new TdengineData();
        tdengineData.setTags(new LinkedHashMap<>());
        for (String k : data.keySet()) {
            switch (k) {
                case "ts":
                    tdengineData.setTs(data.getLong(k));
                    break;
                case "Tags":
                    tdengineData.setTagDataList(parseTags(data.getJSONArray(k)));
                    break;
                default:
                    tdengineData.getTags().put(k, data.getString(k));
            }
        }
        return tdengineData;
    }

    private static List<TdengineTagData> parseTags(JSONArray tags) {
        List<TdengineTagData> tdengineTagDatas = new ArrayList<>();
        Iterator<Object> it = tags.iterator();
        while (it.hasNext()) {
            JSONObject ob = (JSONObject) it.next();
            TdengineTagData tdengineTagData = new TdengineTagData();
            tdengineTagData.setTagName(ob.getString("TagName"));
            tdengineTagData.setValueType(ob.getString("ValueType"));
            tdengineTagData.setTagValue(ob.getString("TagValue"));
            tdengineTagDatas.add(tdengineTagData);
        }
        return tdengineTagDatas;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {

    }

}