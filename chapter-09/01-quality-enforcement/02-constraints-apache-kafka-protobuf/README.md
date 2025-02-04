# Quality enforcement - constraints with Protobuf and Apache Kafka

1. Explain [visit.proto](definitons%2Fvisit.proto)
* the file defines the schema of a visit event but also all data quality constraints associated to each field; you can see among them:
  * required fields (user_id)
  * fields with time-based validation (event_time must be in the past)
  * complex expressions (page cannot end with ".html")
2. Generate the code (it's already present in the _protobuf_output_ directory, you can omit this step):
```
docker run --volume ".:/workspace" --workdir /workspace bufbuild/buf:1.34.0 generate --include-imports
```
3. Explain [visits_generator.py](visits_generator.py):
* the job creates new visits for an Apache Kafka topic
* it initializes several `Visit` instances and performs a validation before sending it to the valid or invalid records topic
  * it relies on the code generated in the previous step; that way, the definition alongside the code can be shared by 
    various data producers helping to achieve data quality at writing instead of at reading time
4. Start Docker container with Apache Kafka:
```
cd docker/
docker-compose down --volumes; docker-compose up
```
5. Start `visits_generator.py`
6. Run `visits_reader.py`. You should see the first two records for visit1 printed as a part of the valid records, and the rest as invalid ones.