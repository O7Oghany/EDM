package net.shirazy.paymentservice.config;

import jakarta.annotation.PostConstruct;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class KafkaConfig {

  @Value("${bootstrap.servers}")
  private String bootstrapServers;

  @Value("${spring.kafka.producer.ssl.trust-store-location}")
  private String trustStorePath;

  @Value("${spring.kafka.producer.ssl.trust-store-password}")
  private String trustStorePassword;

  @PostConstruct
  public void validateTrustStore() {
    // Your validation logic, simplified from the previous example
    File file = new File(trustStorePath);
    if (file.exists()) {
      System.out.println("Trust store exists at: " + trustStorePath);
      // ... Rest of your SSL validation logic
    } else {
      System.err.println("Trust store not found at: " + trustStorePath);
      // Fail fast
      throw new IllegalStateException("Trust store not found");
    }
  }

  private Map<String, Object> producerConfigs() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 3);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
    props.put(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      StringSerializer.class
    );
    props.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      StringSerializer.class
    );
    props.put("security.protocol", "SSL");
    //props.put("ssl.endpoint.identification.algorithm", "");
    props.put("ssl.keystore.type", "PKCS12");
    props.put("ssl.key.password", trustStorePassword);
    props.put("ssl.truststore.location", trustStorePath);
    props.put("ssl.truststore.password", trustStorePassword);
    props.put("debug", "ssl");
    props.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      ByteArraySerializer.class
    );

    return props;
  }

  @Bean
  public ProducerFactory<String, String> producerFactory() {
    return new DefaultKafkaProducerFactory<>(producerConfigs());
  }

  @Bean
  public KafkaTemplate<String, String> kafkaTemplate() {
    return new KafkaTemplate<>(producerFactory());
  }

  @Bean
  public KafkaTemplate<String, byte[]> byteKafkaTemplate() {
    Map<String, Object> props = producerConfigs();
    props.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      ByteArraySerializer.class.getName()
    );
    return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
  }
}
