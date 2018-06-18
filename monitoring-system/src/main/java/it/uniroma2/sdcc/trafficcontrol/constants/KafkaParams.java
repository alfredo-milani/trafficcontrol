package it.uniroma2.sdcc.trafficcontrol.constants;

public interface KafkaParams {

    // Kafka's properties
    String BOOTSTRAP_SERVERS = "bootstrap.servers";
    String GROUP_ID = "group.id";
    String AUTO_COMMIT = "enable.auto.commit";
    String KEY_DESERIALIZER = "key.deserializer";
    String VALUE_DESERIALIZER = "value.deserializer";
    String KEY_SERIALIZER = "key.serializer";
    String VALUE_SERIALIZER = "value.serializer";

    String TRUE_VALUE = "true";
    String FALSE_VALUE = "false";
    Class DESERIALIZER_VALUE = org.apache.kafka.common.serialization.StringDeserializer.class;
    Class SERIALIZER_VALUE = org.apache.kafka.common.serialization.StringSerializer.class;

    // Kafka's topics
    String MONITORING_SOURCE = "monitoring_source";
    String GENERIC_TUPLE_TO_VALIDATE = "generic_tuple_to_validate";
    String SEMAPHORE_SENSOR_VALIDATED = "semaphore_sensor_validated";
    String MOBILE_SENSOR_VALIDATED = "mobile_sensor_validated";
    String SEMAPHORE_LIGHT_STATUS = "semaphore_status";
    String RANKINGS_PROCESSED = "rankings_processed";
    String GREEN_TEMPORIZATION = "green_temporization";
    String ODD_SEMAPHORES = "odd_semaphores";
    String EVEN_SEMAPHORES = "even_semaphores";

    // Kafka's tuples informations
    String KAFKA_TIMESTAMP = "kafka_timestamp";
    String KAFKA_RAW_TUPLE = "kafka_raw_tuple";

}
