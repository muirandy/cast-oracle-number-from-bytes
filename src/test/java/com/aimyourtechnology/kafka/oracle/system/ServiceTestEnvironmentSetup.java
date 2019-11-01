package com.aimyourtechnology.kafka.oracle.system;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

@Testcontainers
public abstract class ServiceTestEnvironmentSetup {

    private static final String KAFKA_KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    private static final String KAFKA_VALUE_DESERIALIZER = "io.confluent.kafka.serializers.KafkaAvroDeserializer";

    private static final String KAFKA_KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String KAFKA_VALUE_SERIALIZER = "io.confluent.kafka.serializers.KafkaAvroSerializer";

    private static final File dockerComposeFile =
            new File(ServiceTestEnvironmentSetup.class.getClassLoader().getResource("docker-compose-service.yml").getFile());

    @Container
    public static DockerComposeContainer container =
            new DockerComposeContainer(dockerComposeFile)
                    .waitingFor("broker_1", Wait.forLogMessage(".*started.*\\n", 1)
                    .withStartupTimeout(Duration.ofSeconds(120)))
                    .withLocalCompose(true);

    protected String randomValue = generateRandomString();
    protected String orderId = generateRandomString();

    @BeforeEach
    void createTopics() {
        AdminClient adminClient = AdminClient.create(getProperties());

        CreateTopicsResult createTopicsResult = adminClient.createTopics(getTopics(), new CreateTopicsOptions().timeoutMs(1000));
        Map<String, KafkaFuture<Void>> futureResults = createTopicsResult.values();
        futureResults.values().forEach(f -> {
            try {
                f.get(2000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        });
        adminClient.close();
    }

    protected List<NewTopic> getTopics() {
        return getTopicNames().stream()
                              .map(n -> new NewTopic(n, 1, (short) 1))
                              .collect(Collectors.toList());
    }

    protected List<String> getTopicNames() {
        List<String> topicNames = new ArrayList<>();
        topicNames.add(getInputTopic());
        topicNames.add(getOutputTopic());
        return topicNames;
    }

    private void waitForDockerEnvironment() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    ProducerRecord createKafkaProducerRecord(Supplier<GenericRecord> avroGenerator) {
        return new ProducerRecord(getInputTopic(), orderId, avroGenerator.get());
    }

    ConsumerRecords<String, String> pollForResults() {
        KafkaConsumer<String, String> consumer = createKafkaConsumer(getProperties());
        Duration duration = Duration.ofSeconds(4);
        return consumer.poll(duration);
    }

    ConsumerRecords<String, GenericRecord> pollForAvroResults() {
        KafkaConsumer<String, GenericRecord> consumer = createKafkaAvroConsumer(getProperties());
        Duration duration = Duration.ofSeconds(4);
        return consumer.poll(duration);
    }

    private KafkaConsumer<String, String> createKafkaConsumer(Properties props) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(getOutputTopic()));
        Duration immediately = Duration.ofSeconds(0);
        consumer.poll(immediately);
        return consumer;
    }

    private KafkaConsumer<String, GenericRecord> createKafkaAvroConsumer(Properties props) {
        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(getOutputTopic()));
        Duration immediately = Duration.ofSeconds(0);
        consumer.poll(immediately);
        return consumer;
    }

    protected abstract String getInputTopic();

    protected abstract String getOutputTopic();

    Properties getProperties() {
        String bootstrapServers = "http://localhost:9092";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put("acks", "all");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KAFKA_KEY_SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KAFKA_VALUE_SERIALIZER);
        props.put("schema.registry.url", "http://localhost:8081");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KAFKA_VALUE_DESERIALIZER);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KAFKA_KEY_DESERIALIZER);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.getClass().getName());
        return props;
    }

    boolean foundExpectedRecord(String key) {
        return orderId.equals(key);
    }

    protected String generateRandomString() {
        return "" + Math.abs(new Random().nextLong());
    }

    protected void writeMessageToInputTopic(Supplier<GenericRecord> avroSupplier) {
        try {
            KafkaProducer<String, GenericRecord> kafkaProducer = new KafkaProducer<>(getProperties());
            ProducerRecord kafkaProducerRecord = createKafkaProducerRecord(avroSupplier);
            kafkaProducer.send(kafkaProducerRecord).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    protected void assertKafkaMessage(Consumer<ConsumerRecord<String, String>> consumerRecordConsumer) {
        ConsumerRecords<String, String> recs = pollForResults();
        assertFalse(recs.isEmpty());

        Spliterator<ConsumerRecord<String, String>> spliterator = Spliterators.spliteratorUnknownSize(recs.iterator(), 0);
        Stream<ConsumerRecord<String, String>> consumerRecordStream = StreamSupport.stream(spliterator, false);
        Optional<ConsumerRecord<String, String>> expectedConsumerRecord = consumerRecordStream.filter(cr -> foundExpectedRecord(cr.key()))
                                                                                              .findAny();
        expectedConsumerRecord.ifPresent(consumerRecordConsumer);
        if (!expectedConsumerRecord.isPresent())
            fail("Did not find expected record");
    }
}
