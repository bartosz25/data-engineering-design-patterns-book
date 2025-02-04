# Fine-grained tracker  - column-level lineage with Apache Spark and OpenLineage

1. Start Docker containers with Marquez:
```
cd ../docker
docker-compose down --volumes; docker-compose up
```

2. The demo follows the Medallion architecture with 3 storage layers (bronze, silver, gold):
* [bronze_table_users_writer.py](bronze_table_users_writer.py) creates the bronze (source) table with users raw data
* [bronze_table_visits_writer.py](bronze_table_visits_writer.py) creates the bronze table for the raw visit events
* [silver_table_enriched_visits_writer.py](silver_table_enriched_visits_writer.py) creates the table with enriched visits
* [gold_table_visits_aggregation_writer.py](gold_table_visits_aggregation_writer.py) creates the table with the visits aggregation

3. Run the `bronze_table_users_writer.py`

4. Run the `bronze_table_visits_writer.py`

5. At this moment we don't have any column lineage as the tables are separate. Let's run the 
`silver_table_enriched_visits_writer.py`

It created a new table that combines the rows of the _bronze_users_ and _bronze_visits_ tables. Although the column
lineage is not yet exposed from the Marquez UI, it's available from the API:
at [http://localhost:3000/api/v1/column-lineage?nodeId=dataset:file:/home/bartosz/workspace/data-engineering-design-patterns-book-private/chapter-10/03-data-lineage/02-fine-grained-tracker-apache-spark-openlineage-marquez/spark-warehouse/enriched_visits&depth=1&withDownstream=false&column=visit_id](http://localhost:3000/api/v1/column-lineage?nodeId=dataset:file:/home/bartosz/workspace/data-engineering-design-patterns-book-private/chapter-10/03-data-lineage/02-fine-grained-tracker-apache-spark-openlineage-marquez/spark-warehouse/enriched_visits&depth=1&withDownstream=false&column=visit_id):
```
{
  "graph": [
  // ...
  {
      "id": "datasetField:file:/home/bartosz/workspace/data-engineering-design-patterns-book-private/chapter-10/03-data-lineage/02-fine-grained-tracker-apache-spark-openlineage-marquez/spark-warehouse/enriched_visits:user_login",
      "type": "DATASET_FIELD",
      "data": {
        "namespace": "file",
        "dataset": "/home/bartosz/workspace/data-engineering-design-patterns-book-private/chapter-10/03-data-lineage/02-fine-grained-tracker-apache-spark-openlineage-marquez/spark-warehouse/enriched_visits",
        "datasetVersion": "001f3b4a-0342-4b79-9750-01eac0a3e174",
        "field": "user_login",
        "fieldType": "string",
        "transformationDescription": null,
        "transformationType": null,
        "inputFields": [
          {
            "namespace": "file",
            "dataset": "/home/bartosz/workspace/data-engineering-design-patterns-book-private/chapter-10/03-data-lineage/02-fine-grained-tracker-apache-spark-openlineage-marquez/spark-warehouse/users",
            "datasetVersion": "f7867151-2970-41ff-9661-05721bf70939",
            "field": "login",
            "transformationDescription": null,
            "transformationType": null
          },
          {
            "namespace": "file",
            "dataset": "/home/bartosz/workspace/data-engineering-design-patterns-book-private/chapter-10/03-data-lineage/02-fine-grained-tracker-apache-spark-openlineage-marquez/spark-warehouse/users",
            "datasetVersion": "f7867151-2970-41ff-9661-05721bf70939",
            "field": "user_id",
            "transformationDescription": null,
            "transformationType": null
          },
          {
            "namespace": "file",
            "dataset": "/home/bartosz/workspace/data-engineering-design-patterns-book-private/chapter-10/03-data-lineage/02-fine-grained-tracker-apache-spark-openlineage-marquez/spark-warehouse/visits",
            "datasetVersion": "a43846af-d911-4f20-b54b-36440f46ab52",
            "field": "user_id",
            "transformationDescription": null,
            "transformationType": null
          }
        ]
      },
      "inEdges": [
        {
          "origin": "datasetField:file:/home/bartosz/workspace/data-engineering-design-patterns-book-private/chapter-10/03-data-lineage/02-fine-grained-tracker-apache-spark-openlineage-marquez/spark-warehouse/enriched_visits:user_login",
          "destination": "datasetField:file:/home/bartosz/workspace/data-engineering-design-patterns-book-private/chapter-10/03-data-lineage/02-fine-grained-tracker-apache-spark-openlineage-marquez/spark-warehouse/users:login"
        },
        {
          "origin": "datasetField:file:/home/bartosz/workspace/data-engineering-design-patterns-book-private/chapter-10/03-data-lineage/02-fine-grained-tracker-apache-spark-openlineage-marquez/spark-warehouse/enriched_visits:user_login",
          "destination": "datasetField:file:/home/bartosz/workspace/data-engineering-design-patterns-book-private/chapter-10/03-data-lineage/02-fine-grained-tracker-apache-spark-openlineage-marquez/spark-warehouse/users:user_id"
        },
        {
          "origin": "datasetField:file:/home/bartosz/workspace/data-engineering-design-patterns-book-private/chapter-10/03-data-lineage/02-fine-grained-tracker-apache-spark-openlineage-marquez/spark-warehouse/enriched_visits:user_login",
          "destination": "datasetField:file:/home/bartosz/workspace/data-engineering-design-patterns-book-private/chapter-10/03-data-lineage/02-fine-grained-tracker-apache-spark-openlineage-marquez/spark-warehouse/visits:user_id"
        }
      ],
      "outEdges": []
    },
```
As you can see, the snippet lists all the dependencies of the user_login, including the _login_ column from `spark-warehouse/users:login`


6. Run the `gold_table_visits_aggregation_writer`.

After executing the query, check the column lineage again at [http://localhost:3000/api/v1/column-lineage?nodeId=dataset:file:/home/bartosz/workspace/data-engineering-design-patterns-book-private/chapter-10/03-data-lineage/02-fine-grained-tracker-apache-spark-openlineage-marquez/spark-warehouse/visits_aggregates&depth=0&withDownstream=true&column=visits_count](http://localhost:3000/api/v1/column-lineage?nodeId=dataset:file:/home/bartosz/workspace/data-engineering-design-patterns-book-private/chapter-10/03-data-lineage/02-fine-grained-tracker-apache-spark-openlineage-marquez/spark-warehouse/visits_aggregates&depth=0&withDownstream=true&column=visits_count):

```
{
  "graph": [
  // ...
  {
      "id": "datasetField:file:/home/bartosz/workspace/data-engineering-design-patterns-book-private/chapter-10/03-data-lineage/02-fine-grained-tracker-apache-spark-openlineage-marquez/spark-warehouse/visits_aggregates:user_id",
      "type": "DATASET_FIELD",
      "data": {
        "namespace": "file",
        "dataset": "/home/bartosz/workspace/data-engineering-design-patterns-book-private/chapter-10/03-data-lineage/02-fine-grained-tracker-apache-spark-openlineage-marquez/spark-warehouse/visits_aggregates",
        "datasetVersion": "faf51492-8fd2-491c-9b50-4670dcaa299a",
        "field": "user_id",
        "fieldType": "string",
        "transformationDescription": null,
        "transformationType": null,
        "inputFields": [
          {
            "namespace": "file",
            "dataset": "/home/bartosz/workspace/data-engineering-design-patterns-book-private/chapter-10/03-data-lineage/02-fine-grained-tracker-apache-spark-openlineage-marquez/spark-warehouse/enriched_visits",
            "datasetVersion": "001f3b4a-0342-4b79-9750-01eac0a3e174",
            "field": "user_id",
            "transformationDescription": null,
            "transformationType": null
          },
          {
            "namespace": "file",
            "dataset": "/home/bartosz/workspace/data-engineering-design-patterns-book-private/chapter-10/03-data-lineage/02-fine-grained-tracker-apache-spark-openlineage-marquez/spark-warehouse/enriched_visits",
            "datasetVersion": "001f3b4a-0342-4b79-9750-01eac0a3e174",
            "field": "visit_id",
            "transformationDescription": null,
            "transformationType": null
          }
        ]
      },
```

As you can notice here, the field comes from both grouping columns in our insert query.