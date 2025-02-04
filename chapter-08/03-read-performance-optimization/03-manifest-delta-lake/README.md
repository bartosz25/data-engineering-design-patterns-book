# Access optimization - Delta Lake manifest

1. Generate the dataset:
```
cd dataset
mkdir -p /tmp/dedp/ch08/03-access-optimization/03-manifest-delta-lake/input/
docker-compose down --volumes; docker-compose up
```

2. Generate the devices Delta Lake table by running the `create_devices_table.py`. It should create three versions in 
the commit log location:
```
$ ls /tmp/dedp/ch08/03-access-optimization/03-manifest-delta-lake/devices-table/_delta_log/
00000000000000000000.json  00000000000000000001.json  00000000000000000002.json
```

3. Explain the [create_devices_table_manifest.py](create_devices_table_manifest.py)
* the code creates a manifest file that lists all the files available for the table 
* it can be used by external engines not able to interpret the commit logs and resolve the table's last version

4. Run the `create_devices_table_manifest.py`. You should see the history of changes printed and a manifest file created
here:
```
$ cat /tmp/dedp/ch08/03-access-optimization/03-manifest-delta-lake/devices-table/_symlink_format_manifest/manifest 
file:/tmp/dedp/ch08/03-access-optimization/03-manifest-delta-lake/devices-table/part-00000-b84a0463-5a41-4788-825a-01db2cf8c2b7-c000.snappy.parquet
file:/tmp/dedp/ch08/03-access-optimization/03-manifest-delta-lake/devices-table/part-00000-4fccf103-1efe-439b-b02c-f9ef4e05c905-c000.snappy.parquet
file:/tmp/dedp/ch08/03-access-optimization/03-manifest-delta-lake/devices-table/part-00000-d1a177f4-282f-44bc-8142-c3e5cd49cf0a-c000.snappy.parquet
```

5. Let's see now what happens when we rerun the steps 3 and 4. The manifest should be updated with the overwritten files:
```
$ cat /tmp/dedp/ch08/03-access-optimization/03-manifest-delta-lake/devices-table/_symlink_format_manifest/manifest 
file:/tmp/dedp/ch08/03-access-optimization/03-manifest-delta-lake/devices-table/part-00000-4bd69cf1-9cba-45a1-801c-b2a5291e4695-c000.snappy.parquet
file:/tmp/dedp/ch08/03-access-optimization/03-manifest-delta-lake/devices-table/part-00000-85e669df-9a10-4245-9a3c-a49c2a7afb2d-c000.snappy.parquet
file:/tmp/dedp/ch08/03-access-optimization/03-manifest-delta-lake/devices-table/part-00000-d6242ad5-3c24-42de-ba8b-3d24a3d7147f-c000.snappy.parquet
```


You can also configure an automatic manifest refresh with 
```
ALTER TABLE delta.`<path-to-delta-table>` SET TBLPROPERTIES(delta.compatibility.symlinkFormatManifest.enabled=true)
```