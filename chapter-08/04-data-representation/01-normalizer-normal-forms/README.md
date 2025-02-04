# Data representation - normalizer - Normal Forms (NF)

1. Generate the dataset:
```
cd dataset
mkdir -p /tmp/dedp/ch08/04-data-representation/01-normalizer-normal-forms/input
docker-compose down --volumes; docker-compose up
```

2. Explain the [normalized_schema_tables_creator.py](normalized_schema_tables_creator.py)
* the job explodes the initial visit event into multiple tables
  * besides, it creates additional tables that were present in other demos of this section 
3. Run `normalized_schema_tables_creator.py`
4. Explain the [normalized_schema_tables_reader.py](normalized_schema_tables_reader.py)
* as you cna see here, to get the full picture yet again, you have to join multiple tables together
5. Run `normalized_schema_tables_reader.py`. You should see all columns read without any joins:
```
+------------------+-----------------+-----------+-------------------+-------------+-----+---------------+----------------+-------------------+-------+---------------+------------+-----------+--------------+------------------+-----------+---------------+--------------------------+
|visit_id          |visit_contexts_id|pages_id   |event_time         |referral     |ad_id|ip             |login           |connected_since    |browser|browser_version|network_type|device_type|device_version|page_categories_id|page       |category_name  |category_url              |
+------------------+-----------------+-----------+-------------------+-------------+-----+---------------+----------------+-------------------+-------+---------------+------------+-----------+--------------+------------------+-----------+---------------+--------------------------+
|140514109234048_0 |1219336607       |568280307  |2023-11-23 16:00:00|Google Ads   |ad 5 |144.250.28.13  |perkinscalvin   |2023-10-28 17:00:00|Firefox|22.0           |4G          |PC         |5.0           |-307125960        |index      |cat_index      |categories/cat_index      |
|140514109234048_1 |-161445224       |-1667375173|2023-11-23 16:00:00|Google Search|NULL |141.222.151.74 |gloverjesse     |NULL               |Firefox|20.1           |Wi-Fi       |MacBook    |3.0           |-358112883        |page_1     |cat_page_1     |categories/cat_page_1     |
|140514109234048_2 |888727511        |568280307  |2023-11-23 16:00:00|Twitter      |NULL |91.170.121.188 |christinaholmes |2023-11-09 16:00:00|Chrome |20.1           |5G          |iPad       |5.0           |-307125960        |index      |cat_index      |categories/cat_index      |
|140514109234048_3 |772088270        |351602982  |2023-11-23 16:00:00|StackOverflow|NULL |191.20.131.218 |delgadojessica  |NULL               |Firefox|20.0           |Wi-Fi       |iPhone     |4.0           |660122411         |page_20    |cat_page_20    |categories/cat_page_20    |
|140514109234048_4 |1012386791       |1973918983 |2023-11-23 16:00:00|YouTube      |ad 4 |171.82.95.110  |lestrada        |NULL               |Safari |24.11          |5G          |Smartphone |4.0           |1124165923        |category_3 |cat_category_3 |categories/cat_category_3 |
|140514109234048_5 |-882414940       |146891777  |2023-11-23 16:00:00|Facebook     |NULL |212.152.213.171|michaellopez    |2023-11-18 16:00:00|Chrome |24.11          |LAN         |iPhone     |6.0           |399705395         |about      |cat_about      |categories/cat_about      |
|140514109234048_6 |409193353        |-168819940 |2023-11-23 16:00:00|Google Ads   |NULL |158.34.100.176 |pmcguire        |NULL               |Safari |20.1           |Wi-Fi       |MacBook    |3.0           |604074138         |category_12|cat_category_12|categories/cat_category_12|
|140514109234048_7 |1735958223       |-1038596635|2023-11-23 16:00:00|Twitter      |ad 1 |28.225.55.105  |manuel76        |NULL               |Safari |21.0           |5G          |iPad       |6.0           |-1457741490       |home       |cat_home       |categories/cat_home       |
|140514109234048_8 |-2012623742      |-1988944783|2023-11-23 16:00:00|Google Search|NULL |80.10.174.210  |debbiegarcia    |NULL               |Chrome |22.0           |Wi-Fi       |iPhone     |6.0           |-1645301217       |page_7     |cat_page_7     |categories/cat_page_7     |
|140514109234048_9 |-72206885        |-1428476315|2023-11-23 16:00:00|Google Ads   |NULL |170.50.253.186 |jonescaitlin    |2023-11-09 16:00:00|Firefox|23.1           |5G          |MacBook    |5.0           |-917330786        |categories |cat_categories |categories/cat_categories |
|140514109234048_10|1762598458       |-1428476315|2023-11-23 16:00:00|Facebook     |NULL |132.198.200.194|ejensen         |NULL               |Chrome |20.2           |5G          |iPhone     |4.0           |-917330786        |categories |cat_categories |categories/cat_categories |
|140514109234048_11|-1390573680      |568280307  |2023-11-23 16:00:00|Medium       |NULL |42.198.165.189 |joel00          |NULL               |Chrome |23.1           |4G          |Smartphone |6.0           |-307125960        |index      |cat_index      |categories/cat_index      |
|140514109234048_12|1093964724       |-1428476315|2023-11-23 16:00:00|Google Ads   |ad 3 |24.119.117.114 |harcher         |NULL               |Chrome |20.2           |4G          |iPhone     |3.0           |-917330786        |categories |cat_categories |categories/cat_categories |
|140514109234048_13|42               |-1667375173|2023-11-23 16:00:00|NULL         |NULL |99.96.207.179  |bassricardo     |NULL               |Safari |23.0           |Wi-Fi       |MacBook    |2.0           |-358112883        |page_1     |cat_page_1     |categories/cat_page_1     |
|140514109234048_13|42               |-1667375173|2023-11-23 16:00:00|NULL         |ad 5 |131.71.74.108  |twatson         |NULL               |Firefox|23.2           |Wi-Fi       |Smartphone |3.0           |-358112883        |page_1     |cat_page_1     |categories/cat_page_1     |
|140514109234048_13|42               |-1667375173|2023-11-23 16:00:00|NULL         |ad 2 |190.240.193.246|millerrebekah   |NULL               |Chrome |22.0           |LAN         |iPad       |3.0           |-358112883        |page_1     |cat_page_1     |categories/cat_page_1     |
|140514109234048_13|42               |-1667375173|2023-11-23 16:00:00|NULL         |NULL |136.220.190.151|jeremy94        |NULL               |Firefox|21.0           |LAN         |MacBook    |5.0           |-358112883        |page_1     |cat_page_1     |categories/cat_page_1     |
|140514109234048_13|42               |-1667375173|2023-11-23 16:00:00|NULL         |NULL |62.251.52.48   |jeffrey27       |NULL               |Safari |23.0           |LAN         |Smartphone |5.0           |-358112883        |page_1     |cat_page_1     |categories/cat_page_1     |
|140514109234048_13|42               |-1667375173|2023-11-23 16:00:00|NULL         |ad 5 |87.10.92.229   |david16         |NULL               |Firefox|20.2           |LAN         |PC         |6.0           |-358112883        |page_1     |cat_page_1     |categories/cat_page_1     |
|140514109234048_13|42               |-1667375173|2023-11-23 16:00:00|NULL         |ad 5 |88.0.34.126    |rodneywashington|NULL               |Firefox|20.1           |5G          |Smartphone |5.0           |-358112883        |page_1     |cat_page_1     |categories/cat_page_1     |
+------------------+-----------------+-----------+-------------------+-------------+-----+---------------+----------------+-------------------+-------+---------------+------------+-----------+--------------+------------------+-----------+---------------+--------------------------+
only showing top 20 rows
```
