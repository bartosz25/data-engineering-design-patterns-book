package com.waitingforcode;

import org.apache.avro.*;

import java.util.Collections;

public class ValidSchemaCompatibility {

    public static void main(String[] args) throws SchemaValidationException {
        Schema schemaWithOptionalAndRemovedField = Schema.parse("{"
                + "\"type\": \"record\","
                + "\"name\": \"order\","
                + "\"fields\": [ {\"name\": \"amount_int\", \"type\": \"int\", \"default\": 1} ]"
                + "}");
        Schema schemaWithRenamedOptionalField = Schema.parse("{"
                + "\"type\": \"record\","
                + "\"name\": \"order\","
                + "\"fields\": [ {\"name\": \"amount\", \"type\": \"double\", \"default\": 0} ]"
                + "}");

        SchemaValidator schemaValidator = new SchemaValidatorBuilder().canReadStrategy().validateLatest();

        schemaValidator.validate(schemaWithRenamedOptionalField, Collections.singletonList(schemaWithOptionalAndRemovedField));
    }

}
