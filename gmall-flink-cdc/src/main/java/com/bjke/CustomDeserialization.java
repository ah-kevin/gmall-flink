package com.bjke;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.source.SourceRecord;

public class CustomDeserialization implements DebeziumDeserializationSchema<String> {
    /**
     * 封装的数据格式
     * {
     *
     * }
     */
    @Override
    public void deserialize(SourceRecord record, Collector<String> out) throws Exception {

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return null;
    }
}
