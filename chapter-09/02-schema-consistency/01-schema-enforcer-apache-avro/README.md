# Schema enforcer - Apache Avro

1. Explain the [ValidSchemaCompatibility.java](src/main/java/com/waitingforcode/ValidSchemaCompatibility.java)
* the code first parses two simple schemas
  * the schemas are compatible thanks to the optional fields 
* next, the code validates whether a record written with the second schema (_schemaWithRenamedOptionalField_)
can be read by a consumer using the first schema (_schemaWithOptionalAndRemovedField_); as we rely on optional fields,
you'll see the code will not fail

2. Run `ValidSchemaCompatibility`

3. Explain the [InvalidSchemaCompatibility.java](src/main/java/com/waitingforcode/InvalidSchemaCompatibility.java)
* it's an opposite example to the previous one as we validate schema with required fields
* the validation should now fail because the required field from the first schema (_schemaWithRequiredAndRemovedField_)
is now missing in the second schema (_schemaWithRenamedRequiredField_)

4. Run `InvalidSchemaCompatibility`; the execution should throw the following exception:

```
Exception in thread "main" org.apache.avro.SchemaValidationException: Unable to read schema: 
{
  "type" : "record",
  "name" : "order",
  "fields" : [ {
    "name" : "amount_int",
    "type" : "int"
  } ]
}
using schema:
{
  "type" : "record",
  "name" : "order",
  "fields" : [ {
    "name" : "amount",
    "type" : "double"
  } ]
}
	at org.apache.avro.ValidateMutualRead.canRead(ValidateMutualRead.java:65)
	at org.apache.avro.ValidateCanRead.validate(ValidateCanRead.java:38)
	at org.apache.avro.ValidateLatest.validate(ValidateLatest.java:49)
	at com.waitingforcode.InvalidSchemaCompatibility.main(InvalidSchemaCompatibility.java:23)
```