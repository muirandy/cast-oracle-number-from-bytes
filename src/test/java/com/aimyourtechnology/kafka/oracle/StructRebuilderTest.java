package com.aimyourtechnology.kafka.oracle;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;

class StructRebuilderTest {
    private Integer expectedInteger = Math.abs(new Random().nextInt());
    private String orderId = "" + Math.abs(new Random().nextLong());

    @Test
    void simpleFieldsAreCopiedWithoutCast() {
        StructRebuilder structRebuilder = new StructRebuilder(createOutputIntegerSchema(), Collections.emptyMap());

        GenericRecord outputMessage = structRebuilder.transformAvroMessage(createOutputAvroMessage());

        assertEquals(orderId, outputMessage.get("id"));
        assertEquals(expectedInteger, outputMessage.get("amount"));
    }

    @Test
    void castIsAppliedToSimpleField() {
        Map<String, String> casts = createCastFor("amount");
        StructRebuilder structRebuilder = new StructRebuilder(createOutputIntegerSchema(), casts);

        GenericRecord outputMessage = structRebuilder.transformAvroMessage(createInputAvroMessage());

        assertEquals(orderId, outputMessage.get("id"));
        assertEquals(expectedInteger, outputMessage.get("amount"));
    }

    private Map<String, String> createCastFor(String amount) {
        Map<String, String> casts = new HashMap<>();
        casts.put(amount, "Integer");
        return casts;
    }

    private GenericRecord createInputAvroMessage() {
        Schema inputSchema = createInputByteSchema();
        GenericRecord message = new GenericData.Record(inputSchema);
        byte[] bytes = new BigInteger("" + expectedInteger).toByteArray();
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        message.put("id", orderId);
        message.put("amount", byteBuffer);
        return message;
    }

    private GenericRecord createOutputAvroMessage() {
        Schema inputSchema = createInputByteSchema();
        GenericRecord message = new GenericData.Record(inputSchema);
        message.put("id", orderId);
        message.put("amount", expectedInteger);
        return message;
    }

    private Schema createOutputIntegerSchema() {
        return SchemaBuilder.record("root")
                .fields()
                .name("id")
                .type(Schema.create(Schema.Type.STRING))
                .noDefault()
                .name("amount")
                .type(Schema.create(Schema.Type.INT))
                .noDefault()
                .endRecord();
    }

    private Schema createInputByteSchema() {
        return SchemaBuilder.record("root")
                .fields()
                .name("id")
                .type(Schema.create(Schema.Type.STRING))
                .noDefault()
                .name("amount")
                .type(Schema.create(Schema.Type.BYTES))
                .noDefault()
                .endRecord();
    }

}