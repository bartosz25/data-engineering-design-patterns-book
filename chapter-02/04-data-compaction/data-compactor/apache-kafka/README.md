1. Start Apache Kafka broker:
```
docker-compose down --volumes; docker-compose up
```
2. Create the tested topic with an aggressive compaction strategy:
```
docker exec compaction_kafka kafka-topics.sh --bootstrap-server localhost:9094 --topic devices --create \
--partitions 1 -config max.compaction.lag.ms=5000  --config min.compaction.lag.ms=5000  \
--config cleanup.policy=compact  --config min.cleanable.dirty.ratio=0.001
```
3. Start the Kafka consumer:
```
docker exec -ti compaction_kafka kafka-console-consumer.sh --bootstrap-server localhost:9094 --topic devices --from-beginning
```
4. Open a new tab and start the Kafka producer:
```
docker exec -ti compaction_kafka  kafka-console-producer.sh  --bootstrap-server localhost:9094 --topic devices --property parse.key=true --property key.separator=,
```
5. Produce the first messages:
```
device1,ABC
device2,DEF
```
6. Wait 10 seconds and produce an updated record for the _device1_, plus some other entries to trigger
the compaction:
```
device1,GHI
device3,D  
d,1
f,5
T,6
```
7. Open a new tab and start a new consumer:
```
$ docker exec -ti compaction_kafka kafka-console-consumer.sh --bootstrap-server localhost:9094 --topic devices --from-beginning
DEF
GHI
D
1
5
6
```

As you can see, the consumer didn't process the `device1,ABC` because of the compaction process triggered with the produced
records. You should also notice the compaction operation in the broker logs:

> compaction_kafka  | [2023-12-24 12:44:04,685] INFO Cleaner 0: Beginning cleaning of log devices-0 (kafka.log.LogCleaner)
> compaction_kafka  | [2023-12-24 12:44:04,685] INFO Cleaner 0: Building offset map for devices-0... (kafka.log.LogCleaner)
> compaction_kafka  | [2023-12-24 12:44:04,714] INFO Cleaner 0: Building offset map for log devices-0 for 2 segments in offset range [2, 6). (kafka.log.LogCleaner)
> compaction_kafka  | [2023-12-24 12:44:04,714] INFO Cleaner 0: Offset map for log devices-0 complete. (kafka.log.LogCleaner)
> compaction_kafka  | [2023-12-24 12:44:04,714] INFO Cleaner 0: Cleaning log devices-0 (cleaning prior to Sun Dec 24 12:44:00 UTC 2023, discarding tombstones prior to upper bound deletion horizon Sat Dec 23 12:42:29 UTC 2023)... (kafka.log.LogCleaner)
> compaction_kafka  | [2023-12-24 12:44:04,715] INFO Cleaner 0: Cleaning LogSegment(baseOffset=0, size=156, lastModifiedTime=1703421749619, largestRecordTimestamp=Some(1703421748616)) in log devices-0 into 0 with an upper bound deletion horizon 1703335349619 computed from the segment last modified time of 1703421749619,retaining deletes. (kafka.log.LogCleaner)
> compaction_kafka  | [2023-12-24 12:44:04,716] INFO Cleaner 0: Cleaning LogSegment(baseOffset=2, size=78, lastModifiedTime=1703421778995, largestRecordTimestamp=Some(1703421777991)) in log devices-0 into 0 with an upper bound deletion horizon 1703335349619 computed from the segment last modified time of 1703421778995,retaining deletes. (kafka.log.LogCleaner)
> compaction_kafka  | [2023-12-24 12:44:04,716] INFO Cleaner 0: Cleaning LogSegment(baseOffset=3, size=216, lastModifiedTime=1703421840571, largestRecordTimestamp=Some(1703421839575)) in log devices-0 into 0 with an upper bound deletion horizon 1703335349619 computed from the segment last modified time of 1703421840571,retaining deletes. (kafka.log.LogCleaner)
> compaction_kafka  | [2023-12-24 12:44:04,720] INFO Cleaner 0: Swapping in cleaned segment LogSegment(baseOffset=0, size=372, lastModifiedTime=1703421840571, largestRecordTimestamp=Some(1703421839575)) for segment(s) List(LogSegment(baseOffset=0, size=156, lastModifiedTime=1703421749619, largestRecordTimestamp=Some(1703421748616)), LogSegment(baseOffset=2, size=78, lastModifiedTime=1703421778995, largestRecordTimestamp=Some(1703421777991)), LogSegment(baseOffset=3, size=216, lastModifiedTime=1703421840571, largestRecordTimestamp=Some(1703421839575))) in log Log(dir=/bitnami/kafka/data/devices-0, topicId=zVr7ny8cQA2k3U3OsVO9sQ, topic=devices, partition=0, highWatermark=7, lastStableOffset=7, logStartOffset=0, logEndOffset=7) (kafka.log.LogCleaner)
> compaction_kafka  | [2023-12-24 12:44:04,727] INFO [kafka-log-cleaner-thread-0]: 
> compaction_kafka  | 	Log cleaner thread 0 cleaned log devices-0 (dirty section = [2, 6])
> compaction_kafka  | 	0.0 MB of log processed in 0.0 seconds (0.0 MB/sec).
> compaction_kafka  | 	Indexed 0.0 MB in 0.0 seconds (0.0 Mb/sec, 70.7% of total time)
> compaction_kafka  | 	Buffer utilization: 0.0%
> compaction_kafka  | 	Cleaned 0.0 MB in 0.0 seconds (0.0 Mb/sec, 29.3% of total time)
> compaction_kafka  | 	Start size: 0.0 MB (6 messages)
> compaction_kafka  | 	End size: 0.0 MB (5 messages)
> compaction_kafka  | 	17.3% size reduction (16.7% fewer messages)
> compaction_kafka  |  (kafka.log.LogCleaner)
> compaction_kafka  | [2023-12-24 12:44:04,727] INFO [kafka-log-cleaner-thread-0]: 	Cleanable partitions: 1, Delayed partitions: 1, max delay: 61693 (kafka.log.LogCleaner)
