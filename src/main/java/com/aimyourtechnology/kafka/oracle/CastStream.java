package com.aimyourtechnology.kafka.oracle;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@ApplicationScoped
public class CastStream {

    @ConfigProperty(name = "schema.registry.url")
    private String schemaRegistryUrl;

    @ConfigProperty(name = "CASTS", defaultValue = "")
    private String castString;


    private static final String APP_NAME = "castOracleNumberFromBytes";
    private static final String INPUT_TOPIC = "input";
    private static final String OUTPUT_TOPIC = "output";
    private Map<String, String> casts;

    @Produces
    public Topology buildTopology() {
        createCasts();
        StreamsBuilder builder = new StreamsBuilder();
        ValueMapper<GenericRecord, GenericRecord> mapper = createValueMapper();
        GenericAvroSerde valueSerde = new GenericAvroSerde();
        Map<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
        serdeConfig.put("schema.registry.url", schemaRegistryUrl);
        valueSerde.configure(serdeConfig, false);
        KStream<String, GenericRecord> inputStream = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(),
                valueSerde));
        KStream<String, GenericRecord> integerCastStream = inputStream.mapValues(mapper);
        integerCastStream.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), valueSerde));
        return builder.build();
    }

    private ValueMapper<GenericRecord, GenericRecord> createValueMapper() {
        Schema outputSchema = obtainSchema(OUTPUT_TOPIC + "-value");
        StructRebuilder structRebuilder = new StructRebuilder(outputSchema, casts);
        Function<GenericRecord, GenericRecord> transformRecord = structRebuilder::transformAvroMessage;
        return transformRecord::apply;
    }

    private void createCasts() {
        String[] casts = castString.split(",");
        this.casts = Arrays.stream(casts)
                .collect(Collectors.toMap(
                        c -> (c.split(":"))[0],
                        c -> (c.split(":"))[1]
                ));
    }

    private Schema obtainSchema(String subject) {
        try {
            return readSchemaFromSchemaRegistry(subject);
        } catch (IOException e) {
            System.err.println(e);
            System.out.println("Unable to contact schema registry(" + schemaRegistryUrl + ") for topic: " + subject);
            throw new SchemaRegistryIoException(e);
        } catch (RestClientException e) {
            System.err.println(e);
            throw new SchemaRegistryClientException(e);
        }
    }

    private Schema readSchemaFromSchemaRegistry(String subject) throws IOException, RestClientException {
        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 20);
        SchemaMetadata schemaMetadata = schemaRegistryClient.getSchemaMetadata(subject, 1);
        return schemaRegistryClient.getById(schemaMetadata.getId());
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
