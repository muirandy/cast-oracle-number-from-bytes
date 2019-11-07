package com.aimyourtechnology.kafka.oracle;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.stream.Collectors;

class StructRebuilder {

    private Schema schema;
    private Map<String, String> casts;

    public StructRebuilder(Schema schema, Map<String, String> casts) {
        this.schema = schema;
        this.casts = casts;
    }

    public GenericRecord transformAvroMessage(GenericRecord inputMessage) {
        GenericRecord message = new GenericData.Record(schema);
        for (Schema.Field field : schema.getFields())
            message.put(field.name(), getSimpleFieldValue(inputMessage, field.name()));
        return message;
    }

    private Object getSimpleFieldValue(GenericRecord inputMessage, String fieldName) {
        if (casts.containsKey(fieldName))
            return castToInteger(inputMessage, fieldName);

        Object field = inputMessage.get(fieldName);

        if (field instanceof GenericRecord)
            return buildNestedRecord(fieldName, (GenericRecord)field);

        return field;
    }

    private GenericRecord buildNestedRecord(String fieldName, GenericRecord nestedRecord) {
        Schema nestedSchema = this.schema.getField(fieldName).schema();
        Map<String, String> nestedCasts = calculateNestedCasts(fieldName);
        return new StructRebuilder(nestedSchema, nestedCasts).transformAvroMessage(nestedRecord);
    }

    private Map<String, String> calculateNestedCasts(String fieldName) {
        return casts.entrySet().stream()
                .filter(e -> e.getKey().startsWith(fieldName + "."))
                .collect(Collectors.toMap(
                        e -> e.getKey().substring(e.getKey().indexOf(".") + 1),
                        e -> e.getValue()
                ));
    }

    private Object castToInteger(GenericRecord inputMessage, String fieldName) {
        ByteBuffer inputField = (ByteBuffer)inputMessage.get(fieldName);
        BigInteger integer = new BigInteger(inputField.array());
        return integer.intValue();
    }

}
