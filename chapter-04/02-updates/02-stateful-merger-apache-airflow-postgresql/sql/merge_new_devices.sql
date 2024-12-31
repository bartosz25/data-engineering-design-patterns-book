MERGE INTO dedp.devices AS d
USING (
    SELECT * FROM dedp.devices_history WHERE execution_time = '{{ ts }}'
) AS c_d
ON c_d.id = d.id
WHEN MATCHED THEN
    UPDATE SET
     brand_name = c_d.brand_name,
     full_name = c_d.full_name,
     processor_brand = c_d.processor_brand
WHEN NOT MATCHED THEN
    INSERT (id, brand_name, full_name, processor_brand)
    VALUES (c_d.id, c_d.brand_name, c_d.full_name, c_d.processor_brand)