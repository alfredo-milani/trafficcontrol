package it.uniroma2.sdcc.trafficcontrol.constants;

public interface StormParams {

    // Spout
    String KAFKA_SPOUT = "kafka_spout";

    // Sensor types
    String SEMAPHORE_SENSOR_STREAM = "semaphore_sensor_stream";
    String MOBILE_SENSOR_STREAM = "mobile_sensor_stream";

    // Validation abstractsBolts
    String SEMAPHORE_SENSOR_AUTH_CACHE_BOLT = "semaphore_sensor_auth_cache_bolt";
    String MOBILE_SENSOR_AUTH_CACHE_BOLT = "mobile_sensor_auth_cache_bolt";
    String CACHE_HIT_STREAM = "cache_hit_stream";
    String CACHE_MISS_STREAM = "cache_miss_stream";
    String SEMAPHORE_AUTH_DB_BOLT = "semaphore_auth_db_bolt";
    String MOBILE_AUTH_DB_BOLT = "mobile_auth_db_bolt";
    String SEMAPHORE_VALIDATION_PUBLISHER_BOLT = "semaphore_validation_publisher_bolt";
    String MOBILE_VALIDATION_PUBLISHER_BOLT = "mobile_validation_publisher_bolt";

    // Semaphore status abstractsBolts
    String SEMAPHORE_STATUS_BOLT = "semaphore_status_bolt";
    String SEMAPHORE_STATUS_PUBLISHER_BOLT = "semaphore_status_publisher_bolt";

    // Ranking Bolts - First query
    String MEAN_SPEED_DISPATCHER_BOLT = "mean_speed_dispatcher_bolt";
    String VALIDATION_DISPATCHER_BOLT = "validation_dispatcher_bolt";
    String MEAN_CALCULATOR_BOLT = "mean_calculator_bolt";
    String INTERSECTION_MEAN_SPEED_OBJECT = "intersection_mean_speed_object";
    String PARTIAL_RANKINGS_OBJECT = "partial_rankable_object";
    String GLOBAL_RANKINGS_OBJECT = "global_rankable_object";
    String PARTIAL_WINDOWED_RANK_BOLT_15_MIN = "partial_windowed_rank_bolt_15_min";
    String GLOBAL_WINDOWED_RANK_BOLT_15_MIN = "global_windowed_rank_bolt_15_min";
    String PARTIAL_WINDOWED_RANK_BOLT_1_H = "partial_windowed_rank_bolt_1_h";
    String GLOBAL_WINDOWED_RANK_BOLT_1_H = "global_windowed_rank_bolt_1_h";
    String PARTIAL_WINDOWED_RANK_BOLT_24_H = "partial_windowed_rank_bolt_24_h";
    String GLOBAL_WINDOWED_RANK_BOLT_24_H = "global_windowed_rank_bolt_24_h";
    String RANK_PUBLISHER_BOLT_15_MIN = "rank_publisher_bolt_15_min";
    String RANK_PUBLISHER_BOLT_1_H = "rank_publisher_bolt_1_h";
    String RANK_PUBLISHER_BOLT_24_H = "rank_publisher_bolt_24_h";

    // Intersection Median - Second query
    String INTERSECTION_MEDIAN_VEHICLES_OBJECT = "intersection_median_vehicles_object";
    String MEDIAN_VEHICLES_DISPATCHER_BOLT = "median_vehicles_dispatcher_bolt";
    String MEDIAN_CALCULATOR_BOLT = "median_calculator_bolt";
    String GLOBAL_MEDIAN_CALCULATOR_BOLT_15_MIN = "global_median_calculator_bolt_15_min";
    String GLOBAL_MEDIAN_CALCULATOR_BOLT_1_H = "global_median_calculator_bolt_1_h";
    String GLOBAL_MEDIAN_CALCULATOR_BOLT_24_H = "global_median_calculator_bolt_24_h";
    String CONGESTED_INTERSECTIONS = "congested_intersections";
    String CONGESTED_INTERSECTIONS_PUBLISHER_BOLT_15_MIN = "congested_intersections_publisher_bolt_15_min";
    String CONGESTED_INTERSECTIONS_PUBLISHER_BOLT_1_H = "congested_intersections_publisher_bolt_1_h";
    String CONGESTED_INTERSECTIONS_PUBLISHER_BOLT_24_H = "congested_intersections_publisher_bolt_24_h";
    String MEDIAN_INTERSECTION_STREAM = "median_intersection_stream";

    // Temporization abstractsBolts
    String GREEN_TIMING_DISPATCHER_BOLT = "green_timing_dispatcher_bolt";
    String FILTER_GREEN_BOLT = "filter_green_bolt";
    String GREEN_SETTER = "green_setter";
    String GREEN_TEMPORIZATION_VALUE = "green_temporization_value";

    // Sequence Bolts - Third query
    String CONGESTED_SEQUENCE_CALCULATOR_BOLT = "congested_sequence_calculator_bolt";
    String SEQUENCES_DISPATCHER_BOLT = "sequences_dispatcher_bolt";
    String SEQUENCE_SELECTOR_BOLT = "sequence_selector_bolt";
    String CONGESTED_SEQUENCE_PUBLISHER_BOLT = "congested_sequence_publisher_bolt";
    String CONGESTION_COMPUTATION_BASE_BOLT = "congestion_computation_bolt";
    String CONGESTION_COMPUTATION_BASE_STREAM = "congestion_computation_stream";
    String SEMAPHORE_SEQUENCE_OBJECT = "semaphore_sequence_object";

}
