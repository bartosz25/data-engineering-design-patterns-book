# Filtering - programmatic API (Spark accumulators)

1. Generate the dataset:
```
cd dataset
rm -rf /tmp/dedp/ch03/filter-interceptor-scala/
mkdir -p /tmp/dedp/ch03/filter-interceptor-scala/input
docker-compose down --volumes; docker-compose up
```
The dataset generates 1000 rows with 80% of rows with data quality issues. 
2. Explain the [FilterWithStatsInterceptor.scala](src%2Fmain%2Fscala%2Fcom%2Fwaitingforcode%2FFilterWithStatsInterceptor.scala)
* the job wraps the filters with accumulators registered in the beginning of the file
* whenever the filter evaluates to false, it increments the associated accumulator
3. Run the `FilterWithStatsInterceptor.scala`
You should see an output like (depending on the generated dataset):
```
Devices without type: 63
Devices with too short name: 0
Devices without full name: 10
Devices without version: 7
```