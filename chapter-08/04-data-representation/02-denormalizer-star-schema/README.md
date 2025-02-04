# Data representation - denormalizer - star schema

1. Generate the dataset:
```
cd dataset
mkdir -p /tmp/dedp/ch08/04-data-representation/02-denormalizer-star-schema/input
docker-compose down --volumes; docker-compose up
```

2. Prepare the base tables by running `prepare_base_tables.py`
3. Explain the [star_schema_tables_creator.py](star_schema_tables_creator.py)
* star schema denormalizes dimension tables
  * for that reason you can see the joins composing them
4. Run `star_schema_tables_creator.py`
5. Explain the [star_schema_tables_reader.py](star_schema_tables_reader.py)
* even though there is a denormalization applied to the dimensions, consumers still need to 