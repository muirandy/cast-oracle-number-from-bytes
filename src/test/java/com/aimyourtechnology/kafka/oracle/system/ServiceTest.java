package com.aimyourtechnology.kafka.oracle.system;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static sun.security.x509.X509CertInfo.SUBJECT;

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
        obtainSchema();

        Consumer<ConsumerRecord<String, GenericRecord>> topicConsumer = cr -> assertRecordValueAvro(cr);
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
    private void obtainSchema() {
        try {
            readSchemaFromSchemaRegistry();
        } catch (IOException e) {
            System.err.println(e);
            throw new SchemaRegistryIoException(e);
        } catch (RestClientException e) {
            System.err.println(e);
            throw new SchemaRegistryClientException(e);
        }
    }

    private void readSchemaFromSchemaRegistry() throws IOException, RestClientException {
        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(getSchemaRegistryUrl(), 20);
        SchemaMetadata schemaMetadata = schemaRegistryClient.getSchemaMetadata(SUBJECT, SUBJECT_VERSION_1);
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
