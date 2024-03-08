package com.waitingforcode;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;

import static org.apache.flink.table.api.Expressions.*;

public class VisitsWithAdsJoinJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment
                .createLocalEnvironmentWithWebUI(new Configuration());
        executionEnvironment.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        executionEnvironment.setParallelism(1); // cause 1 partition on each topic
        executionEnvironment.getCheckpointConfig().disableCheckpointing(); // Disable to focus on the procesisng part

         TableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);

        tableEnvironment.createTemporaryTable("visits_tmp_table", TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                        .fromColumns(SchemaBuilders.forVisits())
                        .watermark("event_time", "event_time - INTERVAL '5' MINUTES")
                        .build())
                .option("topic", "visits")
                .option("properties.bootstrap.servers", "localhost:9094")
                .option("properties.group.id", "visits-consumer")
                .option("value.json.timestamp-format.standard", "ISO-8601")
                .option("value.format", "json")
                .option("properties.auto.offset.reset", "latest")
                .build());
        Table visitsTable = tableEnvironment.from("visits_tmp_table");

        tableEnvironment.createTemporaryTable("ads_tmp_table", TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                        .fromColumns(SchemaBuilders.forAds())
                        .watermark("update_time", "update_time - INTERVAL '5' MINUTE")
                        .build())
                .option("topic", "ads")
                .option("properties.group.id", "ads-consumer")
                .option("properties.bootstrap.servers", "localhost:9094")
                .option("value.format", "json")
                .option("value.json.timestamp-format.standard", "ISO-8601")
                .option("properties.auto.offset.reset", "latest")
                .build());
        Table adsTable = tableEnvironment.from("ads_tmp_table");

        // We consider here the adsTable as a history table
        TemporalTableFunction adsLookupFunction = adsTable.createTemporalTableFunction(
                $("update_time"),
                $("ad_page"));

        tableEnvironment.createTemporarySystemFunction("adsLookupFunction", adsLookupFunction);

        // join with transactions based on the time attribute and key
        Table joinResult = visitsTable
                .joinLateral(call("adsLookupFunction",
                                $("event_time")),
                        $("ad_page").isEqual($("page")))
                .select($("*"));

        tableEnvironment.createTable("visits_with_ads", TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                        .fromColumns(SchemaBuilders.forVisitsWithAds())
                        .build())
                .option("topic", "visits-with-ads")
                .option("properties.bootstrap.servers", "localhost:9094")
                .option("key.fields", "visit_id")
                .option("key.format", "raw")
                .option("value.json.timestamp-format.standard", "ISO-8601")
                .option("value.format", "json")
                .build());

        joinResult.insertInto("visits_with_ads").execute();
    }
}
