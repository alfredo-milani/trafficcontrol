package it.uniroma2.sdcc.trafficcontrol.constants;

public interface StormParams {

    // Spout
    String KAFKA_SPOUT = "kafkaSpout";

    // Sensor type
    String SEMAPHORE_SENSOR_STREAM = "semaphore_sensor_stream";
    String MOBILE_SENSOR_STREAM = "mobile_sensor_stream";

    // Validation bolts
    String VALIDITY_CHECK_BOLT = "validityCheckBolt";
    String SEMAPHORE_SENSOR_AUTH_CACHE_BOLT = "semaphore_sensor_auth_cache_bolt";
    String MOBILE_SENSOR_AUTH_CACHE_BOLT = "mobile_sensor_auth_cache_bolt";
    String CACHE_HIT_STREAM = "cache_hit_stream";
    String CACHE_MISS_STREAM = "cache_miss_stream";
    String SEMAPHORE_AUTH_DB_BOLT = "semaphore_auth_db_bolt";
    String MOBILE_AUTH_DB_BOLT = "mobile_auth_db_bolt";
    String SEMAPHORE_VALIDATION_PUBLISHER_BOLT = "semaphore_validation_publisher_bolt";
    String MOBILE_VALIDATION_PUBLISHER_BOLT = "mobile_validation_publisher_bolt";

    // Semaphore status bolts
    String SEMAPHORE_STATUS_BOLT = "semaphore_status_bolt";
    String SEMAPHORE_STATUS_PUBLISHER_BOLT = "semaphore_status_publisher_bolt";
    String REMOVE_STREAM = "remove_stream";
    String UPDATE_STREAM = "update_stream";

    // Ranking Bolts
    String MEAN_SPEED_DISPATCHER_BOLT = "mean_speed_dispatcher_bolt";
    String VALIDATION_DISPATCHER_BOLT = "validation_dispatcher_bolt";
    String MEAN_CALCULATOR_BOLT = "mean_calculator_bolt";
    String INTERSECTION_MEAN_SPEED_OBJECT = "intersection_mean_speed_object";
    String PARTIAL_RANKINGS_OBJECT = "partial_rankable_object";
    String GLOBAL_RANKINGS_OBJECT = "global_rankable_object";
    String PARTIAL_WINDOWED_RANK_BOLT = "partial_windowed_rank_bolt";
    String GLOBAL_WINDOWED_RANK_BOLT = "global_windowed_rank_bolt";

    // per seconda query
    String INTERSECTION_AND_GLOBAL_MEDIAN = "intersection_and_global_median";
    String FINAL_COMPARATOR = "final_comparator";
    String FIELDS_SELECTION_FOR_MEDIAN = "fields_selector_for_median";
    String REMOVE_GLOBAL_MEDIAN_ITEM = "remove_global_median_item";
    String UPDATE_GLOBAL_MEDIAN = "update_global_median";
    String VALUE_GLOBAL_MEDIAN = "value_global_median";

    //Green Setting bolts
    String GREEN_TIMING_DISPATCHER_BOLT = "green_timing_dispatcher_bolt";
    String FILTER_GREEN_BOLT = "filter_green_bolt";
    String GREEN_SETTER = "green_setter";
    String GREEN_TEMPORIZATION_VALUE = "green_temporization_value";


    String UPDATE_PARTIAL = "update";
    String REMOVE = "remove";

}
