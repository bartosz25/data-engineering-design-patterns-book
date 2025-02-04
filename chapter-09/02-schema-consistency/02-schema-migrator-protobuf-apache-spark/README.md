# Schema consistency - migration with Protobuf and Apache Kafka

1. Prepare the directories:
```
rm -rf /tmp/dedp/ch09/02-schema-consistency/02-schema-migrator-protobuf-apache-spark/
mkdir -p /tmp/dedp/ch09/02-schema-consistency/02-schema-migrator-protobuf-apache-spark/
```

2. Explain [visit.proto](definitons%2Fvisit.proto)
* our schema has 8 fields and some of them have to be reworked:
  * _from_page_ is a pretty confusing name for the visit's origin, we want to change it to _referral_
  * user properties (_user_id_, _login_, _is_connected_, _ip_) are spread in the schema, we want to group them in the 
    single structure called _user_details_ 

3. Generate the code (it's already present in the _protobuf_output_ directory, you can omit this step):
```
# Python Visit class builder
docker run --volume ".:/workspace" --workdir /workspace bufbuild/buf:1.34.0 generate --include-imports
# Protobuf binary descriptor used by Apache Spark's from_protobuf
docker run --volume ".:/workspace" --workdir /workspace bufbuild/buf:1.34.0 build -o protobuf_output/visit.bin
```

⚠️ For the sake of simplicity, this operation will also generate other schema versions

4. Explain [visits_generator.py](visits_generator.py):
* the code writes visits to an Apache Kafka topic in the first format

5. Explain [visits_reader.py](visits_reader.py)
* this is our data consumer
  * it processes data continuously from the visits topic 
  * the job converts the raw binary bytes from the topic's value into a structure corresponding to the Protobuf message
    * it uses for that the `from_protobuf` function and the binary descriptor file generated in one of the previous steps
  * later, the job filters out all anonymous (not connected) users and prints them on the screen
* as you can see, if we evolve the schema on the producer side and move all user-related attributes to a dedicated
  structure, the consumer will not process any data, as the filtering field won't be there anymore
  * to mitigate the issue, we're going to use the migration strategy

6. Start Docker container with Apache Kafka:
```
cd docker/
docker-compose down --volumes; docker-compose up
```

7. Start `visits_generator.py`.

8. Run `visits_reader.py`. 
It should print the first generated visits:
```
-- connected visitors --
+--------+--------------------------+-------+------------+--------+--------+------------+----------------+
|visit_id|event_time                |user_id|page        |ip      |login   |is_connected|from_page       |
+--------+--------------------------+-------+------------+--------+--------+------------+----------------+
|visit1  |2024-08-08 05:20:34.230876|u1     |contact     |1.1.1.u1|login u1|true        |origin u1 visit1|
|visit3  |2024-08-08 05:20:34.230876|u2     |contact     |1.1.1.u2|login u2|true        |origin u2 visit3|
|visit4  |2024-08-08 05:20:34.230876|u3     |contact.html|1.1.1.u3|login u3|true        |origin u3 visit4|
|visit1  |2024-08-08 05:20:34.230876|u1     |index       |1.1.1.u1|login u1|true        |origin u1 visit1|
|visit1  |2024-08-08 05:20:34.230876|u1     |contact     |1.1.1.u1|login u1|true        |origin u1 visit1|
|visit5  |2024-08-08 05:20:34.230876|u3     |contact     |1.1.1.u3|login u3|true        |origin u3 visit5|
+--------+--------------------------+-------+------------+--------+--------+------------+----------------+

-- all visits --
+--------+--------------------------+-------+------------+--------+--------+------------+----------------+
|visit_id|event_time                |user_id|page        |ip      |login   |is_connected|from_page       |
+--------+--------------------------+-------+------------+--------+--------+------------+----------------+
|visit1  |2024-08-08 05:20:34.230876|u1     |contact     |1.1.1.u1|login u1|true        |origin u1 visit1|
|visit3  |2024-08-08 05:20:34.230876|u2     |contact     |1.1.1.u2|login u2|true        |origin u2 visit3|
|visit4  |2024-08-08 05:20:34.230876|u3     |contact.html|1.1.1.u3|login u3|true        |origin u3 visit4|
|visit1  |2024-08-08 05:20:34.230876|u1     |index       |1.1.1.u1|login u1|true        |origin u1 visit1|
|visit1  |2024-08-08 05:20:34.230876|u1     |contact     |1.1.1.u1|login u1|true        |origin u1 visit1|
|visit5  |2024-08-08 05:20:34.230876|u3     |contact     |1.1.1.u3|login u3|true        |origin u3 visit5|
+--------+--------------------------+-------+------------+--------+--------+------------+----------------+

```

9. Let's keep the reader running and meantime, evolve the schema. The new definition is defined in [visit_v2.proto](definitons%2Fvisit_v2.proto)
* _from_page_ was renamed to the _referral_
  * ⚠️ The rename consists of changing the field name but **keeping the number**
    * a Protobuf message is index-based, i.e. it doesn't pass the field names but the associated indexes; that way
      the rename is considered as a safe operation
* user-related attributes were grouped into _user_details_ object
  * consequently, they were removed from the original message

10. Run `visits_generator_v2.py`
The consumer should return an empty dataset for the connected users but as you can see in the _all visits_ dataset,
the referral column, in our job called _from_page_, is correctly interpreted thanks to the index-based Protobuf resolution:
```
-- connected visitors --
+--------+----------+-------+----+---+-----+------------+---------+
|visit_id|event_time|user_id|page|ip |login|is_connected|from_page|
+--------+----------+-------+----+---+-----+------------+---------+
+--------+----------+-------+----+---+-----+------------+---------+

-- all visits --
+--------+--------------------------+-------+------------+----+-----+------------+----------------+
|visit_id|event_time                |user_id|page        |ip  |login|is_connected|from_page       |
+--------+--------------------------+-------+------------+----+-----+------------+----------------+
|visit1  |2024-08-08 05:21:22.820808|NULL   |index       |NULL|NULL |NULL        |origin u1 visit1|
|visit1  |2024-08-08 05:21:22.820808|NULL   |contact     |NULL|NULL |NULL        |origin u1 visit1|
|visit1  |2024-08-08 05:21:22.820808|NULL   |contact     |NULL|NULL |NULL        |origin u1 visit1|
|visit3  |2024-08-08 05:21:22.820808|NULL   |contact     |NULL|NULL |NULL        |origin u2 visit3|
|visit4  |2024-08-08 05:21:22.820808|NULL   |contact.html|NULL|NULL |NULL        |origin u3 visit4|
|visit5  |2024-08-08 05:21:22.820808|NULL   |contact     |NULL|NULL |NULL        |origin u3 visit5|
+--------+--------------------------+-------+------------+----+-----+------------+----------------+
```

However, the user details are not available as they were moved to another field.

11. Let's follow now the migrator pattern that keeps old and new records. 
Check the [visit_v3.proto](definitons%2Fvisit_v3.proto) file.
* as you can see, the renamed and changed fields are present alongside their old counterparts

12. Run `visits_generator_v3.py`. After the run, the consumer should process new data:

```
-- connected visitors --
+--------+--------------------------+-------+------------+--------+--------+------------+----------------+
|visit_id|event_time                |user_id|page        |ip      |login   |is_connected|from_page       |
+--------+--------------------------+-------+------------+--------+--------+------------+----------------+
|visit3  |2024-08-08 05:21:58.262216|u2     |contact     |1.1.1.u2|login u2|true        |origin u2 visit3|
|visit4  |2024-08-08 05:21:58.262216|u3     |contact.html|1.1.1.u3|login u3|true        |origin u3 visit4|
|visit5  |2024-08-08 05:21:58.262216|u3     |contact     |1.1.1.u3|login u3|true        |origin u3 visit5|
|visit1  |2024-08-08 05:21:58.262216|u1     |index       |1.1.1.u1|login u1|true        |origin u1 visit1|
|visit1  |2024-08-08 05:21:58.262216|u1     |contact     |1.1.1.u1|login u1|true        |origin u1 visit1|
|visit1  |2024-08-08 05:21:58.262216|u1     |contact     |1.1.1.u1|login u1|true        |origin u1 visit1|
+--------+--------------------------+-------+------------+--------+--------+------------+----------------+

-- all visits --
+--------+--------------------------+-------+------------+--------+--------+------------+----------------+
|visit_id|event_time                |user_id|page        |ip      |login   |is_connected|from_page       |
+--------+--------------------------+-------+------------+--------+--------+------------+----------------+
|visit3  |2024-08-08 05:21:58.262216|u2     |contact     |1.1.1.u2|login u2|true        |origin u2 visit3|
|visit4  |2024-08-08 05:21:58.262216|u3     |contact.html|1.1.1.u3|login u3|true        |origin u3 visit4|
|visit5  |2024-08-08 05:21:58.262216|u3     |contact     |1.1.1.u3|login u3|true        |origin u3 visit5|
|visit1  |2024-08-08 05:21:58.262216|u1     |index       |1.1.1.u1|login u1|true        |origin u1 visit1|
|visit1  |2024-08-08 05:21:58.262216|u1     |contact     |1.1.1.u1|login u1|true        |origin u1 visit1|
|visit1  |2024-08-08 05:21:58.262216|u1     |contact     |1.1.1.u1|login u1|true        |origin u1 visit1|
+--------+--------------------------+-------+------------+--------+--------+------------+----------------+
```

13. Explain the [visits_reader_v2.py](visits_reader_v2.py):
* the job uses new binary descriptor file (`visit_v2.bin`), message name (`messageName='VisitV2'`), and 
adapted filter condition

14. Stop `visits_reader.py` and start the `visits_reader_v2.py`.
15. Run `visits_generator_v2.py`. The consumer should process new visits:

```
-- connected visitors --
+--------+--------------------------+------------+----------------+------------------------------+
|visit_id|event_time                |page        |referral        |user_details                  |
+--------+--------------------------+------------+----------------+------------------------------+
|visit4  |2024-08-08 05:28.809323|contact.html|origin u3 visit4|{u3, 1.1.1.u3, login u3, true}|
|visit1  |2024-08-08 05:28.809323|index       |origin u1 visit1|{u1, 1.1.1.u1, login u1, true}|
|visit1  |2024-08-08 05:28.809323|contact     |origin u1 visit1|{u1, 1.1.1.u1, login u1, true}|
|visit1  |2024-08-08 05:28.809323|contact     |origin u1 visit1|{u1, 1.1.1.u1, login u1, true}|
|visit3  |2024-08-08 05:28.809323|contact     |origin u2 visit3|{u2, 1.1.1.u2, login u2, true}|
|visit5  |2024-08-08 05:28.809323|contact     |origin u3 visit5|{u3, 1.1.1.u3, login u3, true}|
+--------+--------------------------+------------+----------------+------------------------------+

-- all visits --
+--------+--------------------------+------------+----------------+------------------------------+
|visit_id|event_time                |page        |referral        |user_details                  |
+--------+--------------------------+------------+----------------+------------------------------+
|visit4  |2024-08-08 05:28.809323|contact.html|origin u3 visit4|{u3, 1.1.1.u3, login u3, true}|
|visit1  |2024-08-08 05:28.809323|index       |origin u1 visit1|{u1, 1.1.1.u1, login u1, true}|
|visit1  |2024-08-08 05:28.809323|contact     |origin u1 visit1|{u1, 1.1.1.u1, login u1, true}|
|visit1  |2024-08-08 05:28.809323|contact     |origin u1 visit1|{u1, 1.1.1.u1, login u1, true}|
|visit3  |2024-08-08 05:28.809323|contact     |origin u2 visit3|{u2, 1.1.1.u2, login u2, true}|
|visit5  |2024-08-08 05:28.809323|contact     |origin u3 visit5|{u3, 1.1.1.u3, login u3, true}|
+--------+--------------------------+------------+----------------+------------------------------+
```
