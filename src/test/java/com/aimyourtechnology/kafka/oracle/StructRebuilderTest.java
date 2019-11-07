package com.aimyourtechnology.kafka.oracle;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

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

    @Test
    void nestedSimpleFieldsAreCopiedWithoutCast() {
        StructRebuilder structRebuilder = new StructRebuilder(createOutputNestedIntegerSchema(),
                Collections.emptyMap());

        GenericRecord outputMessage = structRebuilder.transformAvroMessage(createNestedOutputAvroMessage());

        assertEquals(orderId, outputMessage.get("id"));
        assertEquals(expectedInteger, outputMessage.get("amount"));

        GenericRecord nestedRecord = (GenericRecord) outputMessage.get("A");
        assertEquals(orderId, nestedRecord.get("id"));
        assertEquals(expectedInteger, nestedRecord.get("amount"));
    }

    @Test
    void castIsAppliedToNestedFields() {
        Map<String, String> casts = createCastFor("amount", "A.amount");
        StructRebuilder structRebuilder = new StructRebuilder(createOutputNestedIntegerSchema(), casts);

        GenericRecord outputMessage = structRebuilder.transformAvroMessage(createNestedInputAvroMessage());

        assertEquals(orderId, outputMessage.get("id"));
        assertEquals(expectedInteger, outputMessage.get("amount"));

        GenericRecord nestedRecord = (GenericRecord) outputMessage.get("A");
        assertEquals(orderId, nestedRecord.get("id"));
        assertEquals(expectedInteger, nestedRecord.get("amount"));
    }

    private GenericRecord createNestedInputAvroMessage() {
        Schema inputSchema = createInputNestedByteSchema();
        GenericRecord message = new GenericData.Record(inputSchema);
        byte[] bytes = new BigInteger("" + expectedInteger).toByteArray();
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        message.put("id", orderId);
        message.put("amount", byteBuffer);

        GenericRecord nestedRecord = new GenericData.Record(inputSchema.getField("A").schema());
        nestedRecord.put("id", orderId);
        nestedRecord.put("amount", byteBuffer);

        message.put("A", nestedRecord);
        return message;
    }

    private GenericRecord createNestedOutputAvroMessage() {
        Schema outputSchema = createOutputNestedIntegerSchema();
        GenericRecord message = new GenericData.Record(outputSchema);
        message.put("id", orderId);
        message.put("amount", expectedInteger);

        GenericRecord nestedRecord = new GenericData.Record(outputSchema.getField("A").schema());
        nestedRecord.put("id", orderId);
        nestedRecord.put("amount", expectedInteger);

        message.put("A", nestedRecord);
        return message;
    }

    private Schema createOutputNestedIntegerSchema() {
        Schema nested = SchemaBuilder.record("A")
                .fields()
                .requiredString("id")
                .requiredInt("amount")
                .endRecord();

        return SchemaBuilder.record("root")
                .fields()
                .requiredString("id")
                .requiredInt("amount")
                .name("A").type(nested).noDefault()
                .endRecord();
    }

    private Schema createInputNestedByteSchema() {
        Schema nested = SchemaBuilder.record("A")
                .fields()
                .requiredString("id")
                .requiredBytes("amount")
                .endRecord();

        return SchemaBuilder.record("root")
                .fields()
                .requiredString("id")
                .requiredBytes("amount")
                .name("A").type(nested).noDefault()
                .endRecord();
    }

    private Map<String, String> createCastFor(String... cast) {
        Map<String, String> casts = new HashMap<>();
        for (String c : cast)
            casts.put(c, "Integer");
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