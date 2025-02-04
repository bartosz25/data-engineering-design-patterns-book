package com.waitingforcode;

import org.apache.avro.*;

import java.util.Collections;

public class InvalidSchemaCompatibility {

    public static void main(String[] args) throws SchemaValidationException {
        Schema schemaWithRequiredAndRemovedField = Schema.parse("{"
                + "\"type\": \"record\","
                + "\"name\": \"order\","
                + "\"fields\": [ {\"name\": \"amount_int\", \"type\": \"int\"} ]"
                + "}");
        Schema schemaWithRenamedRequiredField = Schema.parse("{"
                + "\"type\": \"record\","
                + "\"name\": \"order\","
                + "\"fields\": [ {\"name\": \"amount\", \"type\": \"double\"} ]"
                + "}");

        SchemaValidator schemaValidator = new SchemaValidatorBuilder().canReadStrategy().validateLatest();

        schemaValidator.validate(schemaWithRenamedRequiredField, Collections.singletonList(schemaWithRequiredAndRemovedField));
    }

}
