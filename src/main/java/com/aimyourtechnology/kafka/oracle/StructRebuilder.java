package com.aimyourtechnology.kafka.oracle;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Map;

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
        return inputMessage.get(fieldName);
    }

    private Object castToInteger(GenericRecord inputMessage, String fieldName) {
        ByteBuffer inputField = (ByteBuffer)inputMessage.get(fieldName);
        BigInteger integer = new BigInteger(inputField.array());
        return integer.intValue();
    }

}
