package com.aimyourtechnology.kafka.oracle.service;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ServiceShould extends ServiceTestEnvironmentSetup {
    private static final String INPUT_TOPIC = "input";
    private static final String OUTPUT_TOPIC = "output";
    private static final int SUBJECT_VERSION_1 = 1;
    private static final int EXPECTED_INTEGER = 1000;

    @Test
    void bytesAreCastToInteger() {
        createSchemaForInputTopic();
        createSchemaForOutputTopic();
        writeMessageToInputTopic(this::createInputAvroMessage);
        assertAvroMessageHasAppliedCast();
    }

    private void createSchemaForInputTopic() {
        createSchema(INPUT_TOPIC + "-value", createInputByteSchema());
    }

    private void createSchema(String subject, Schema schema) {
        try {
            SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(getSchemaRegistryUrl(), 20);
            schemaRegistryClient.register(subject, schema);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (RestClientException e) {
            e.printStackTrace();
        }
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

    private void createSchemaForOutputTopic() {
        createSchema(OUTPUT_TOPIC + "-value", createOutputIntegerSchema());
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

    private void assertAvroMessageHasAppliedCast() {
        assertValueContainsInteger();
    }

    private void assertValueContainsInteger() {
        ConsumerRecords<String, GenericRecord> records = pollForAvroResults();
        assertEquals(1, records.count());
        ConsumerRecord<String, GenericRecord> outputRecord = records.iterator().next();
        assertEquals(orderId, outputRecord.key());
        GenericRecord value = outputRecord.value();
        Integer integer = (Integer)value.get("amount");
        assertEquals(EXPECTED_INTEGER, integer);
    }

    private GenericRecord createInputAvroMessage() {
        Schema inputSchema = obtainSchema(INPUT_TOPIC + "-value");
        GenericRecord message = new GenericData.Record(inputSchema);
        byte[] bytes = new BigInteger("" + EXPECTED_INTEGER).toByteArray();
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        message.put("id", orderId);
        message.put("amount", byteBuffer);
        return message;
    }

    private Schema obtainSchema(String subject) {
        try {
            return readSchemaFromSchemaRegistry(subject);
        } catch (IOException e) {
            System.err.println(e);
            throw new SchemaRegistryIoException(e);
        } catch (RestClientException e) {
            System.err.println(e);
            throw new SchemaRegistryClientException(e);
        }
    }

    private Schema readSchemaFromSchemaRegistry(String subject) throws IOException, RestClientException {
        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(getSchemaRegistryUrl(), 20);
        SchemaMetadata schemaMetadata = schemaRegistryClient.getSchemaMetadata(subject, SUBJECT_VERSION_1);
        return schemaRegistryClient.getById(schemaMetadata.getId());
    }

    private void assertRecordValueAvro(ConsumerRecord<String, GenericRecord> consumerRecord) {
        GenericRecord value = consumerRecord.value();
        GenericRecord expectedValue = createExpectedAvroMessage();
        assertEquals(expectedValue, value);
    }

    private GenericRecord createExpectedAvroMessage() {
        Schema schema = obtainSchema(OUTPUT_TOPIC + "-value");
        GenericRecord message = new GenericData.Record(schema);
        message.put("id", orderId);
        message.put("amount", 1000);
        return message;
    }

    @Override
    public String getInputTopic() {
        return INPUT_TOPIC;
    }

    @Override
    protected String getOutputTopic() {
        return OUTPUT_TOPIC;
    }

    private class SchemaRegistryIoException extends RuntimeException {
        public SchemaRegistryIoException(IOException e) {
        }
    }

    private class SchemaRegistryClientException extends RuntimeException {
        public SchemaRegistryClientException(RestClientException e) {
        }
    }

}
