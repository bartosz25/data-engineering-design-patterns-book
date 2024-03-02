package com.becomedataengineer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class RecentVisitsInfoPreparatorJob {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        localEnvironment.setParallelism(2);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:29092")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setTopics("visits")
                .build();

        DataStreamSource<String> kafkaDataStreamSource = localEnvironment.fromSource(kafkaSource,
                WatermarkStrategy.<String>forMonotonousTimestamps().withTimestampAssigner(
                        new VisitTimestampAssigner.Supplier()
                ),
                "Kafka Source");

        SingleOutputStreamOperator<Visit> visits = kafkaDataStreamSource.map((String inputRecord) ->
                Json.MAPPER.readValue(inputRecord, Visit.class));

        WindowedStream<Visit, Integer, TimeWindow> tumblingVisitWindow = visits.keyBy(Visit::getVisitId).window(
                TumblingEventTimeWindows.of(Time.seconds(15))
        ).allowedLateness(
                Time.seconds(0)
        ).trigger(
                ContinuousEventTimeTrigger.of(Time.seconds(7))
        );

        SingleOutputStreamOperator<VisitsPerWindow> windowOutput = tumblingVisitWindow.process(new ProcessWindowFunction<Visit, VisitsPerWindow, Integer, TimeWindow>() {
            @Override
            public void process(Integer visitId,
                                ProcessWindowFunction<Visit, VisitsPerWindow, Integer, TimeWindow>.Context context,
                                Iterable<Visit> elements,
                                Collector<VisitsPerWindow> outputCollector) {
                outputCollector.collect(
                        VisitsPerWindow.valueOf(visitId, context.window(), elements, context.currentProcessingTime())
                );
            }
        });

        windowOutput.print();
        localEnvironment.execute();
    }

}
