package com.aimyourtechnology.kafka.oracle.system;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ServiceTest extends ServiceTestEnvironmentSetup {
    private static final String INPUT_TOPIC = "input";
    private static final String OUTPUT_TOPIC = "output";
    private static final int SUBJECT_VERSION_1 = 1;

    private Schema schema;

    @Test
    void t() {
        assertAvroMessageHasAppliedCast();
    }

    private void assertAvroMessageHasAppliedCast() {
        assertValueContainsInteger();
    }

    private void assertValueContainsInteger() {
        ConsumerRecords<String, GenericRecord> records = pollForAvroResults();
        assertEquals(1, records.count());
        //        Consumer<ConsumerRecord<String, GenericRecord>> topicConsumer = cr -> assertRecordValueAvro(cr);
//        assertAvroKafkaMessage(consumerRecordConsumer);
    }

    private void assertRecordValueAvro(ConsumerRecord<String, GenericRecord> consumerRecord) {
        GenericRecord value = consumerRecord.value();
        GenericRecord expectedValue = createAvroMessage();
        assertEquals(expectedValue, value);
    }


    private GenericRecord createAvroMessage() {
        GenericRecord message = new GenericData.Record(schema);
        message.put("id", orderId);
        message.put("amount", 1000);
        return message;
    }

    private void obtainSchema(String subject) {
        try {
            readSchemaFromSchemaRegistry(subject);
        } catch (IOException e) {
            System.err.println(e);
            throw new SchemaRegistryIoException(e);
        } catch (RestClientException e) {
            System.err.println(e);
            throw new SchemaRegistryClientException(e);
        }
    }

    private void readSchemaFromSchemaRegistry(String subject) throws IOException, RestClientException {
        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(getSchemaRegistryUrl(), 20);
        SchemaMetadata schemaMetadata = schemaRegistryClient.getSchemaMetadata(subject, SUBJECT_VERSION_1);
        schema = schemaRegistryClient.getById(schemaMetadata.getId());
    }

    private String getSchemaRegistryUrl() {
        return "http://localhost:8081";
    }

    private class SchemaRegistryIoException extends RuntimeException {
        public SchemaRegistryIoException(IOException e) {
        }
    }

    private class SchemaRegistryClientException extends RuntimeException {
        public SchemaRegistryClientException(RestClientException e) {
        }
    }

    @Override
    public String getInputTopic() {
        return INPUT_TOPIC;
    }

    @Override
    protected String getOutputTopic() {
        return OUTPUT_TOPIC;
    }
}
