package it.uniroma2.sdcc.trafficcontrol.constants;

public interface KafkaParams {

    // Kafka's properties
    String SERVER = "bootstrap.servers";
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
    String RANKING_DESTINATION = "ranking_destination";

    String MONITORING_QUERY1 = "monitoring_query1";
    String MONITORING_QUERY3_LAMP_HOURLY = "monitoring_query3_lamp_hourly";
    String MONITORING_QUERY3_ROAD_HOURLY = "monitoring_query3_road_hourly";
    String MONITORING_QUERY3_CITY_HOURLY = "monitoring_query3_city_hourly";
    String MONITORING_QUERY3_LAMP_DAILY = "monitoring_query3_lamp_daily";
    String MONITORING_QUERY3_ROAD_DAILY = "monitoring_query3_road_daily";
    String MONITORING_QUERY3_CITY_DAILY = "monitoring_query3_city_daily";
    String MONITORING_QUERY3_LAMP_WEEKLY = "monitoring_query3_lamp_weekly";
    String MONITORING_QUERY3_ROAD_WEEKLY = "monitoring_query3_road_weekly";
    String MONITORING_QUERY3_CITY_WEEKLY = "monitoring_query3_city_weekly";
    String MONITORING_QUERY4 = "monitoring_query4";
    String SEMAPHORE_STATUS = "semaphore_status";

    // Kafka's tuples informations
    String KAFKA_TIMESTAMP = "kafka_timestamp";
    String KAFKA_RAW_TUPLE = "kafka_raw_tuple";

}
