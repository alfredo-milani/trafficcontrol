package it.uniroma2.sdcc.trafficcontrol.constants;

public interface StormParams {

    // Spout
    String KAFKA_SPOUT = "kafkaSpout";

    // Validation bolts
    String VALIDITY_CHECK_BOLT = "validityCheckBolt";
    String AUTHENTICATION_CACHE_BOLT = "authentication_cache_bolt";
    String CACHE_HIT_STREAM = "cache_hit_stream";
    String CACHE_MISS_STREAM = "cache_miss_stream";
    String AUTHENTICATION_BOLT = "authentication_bolt";
    String VALIDATION_PUBLISHER_BOLT = "validation_publisher_bolt";

    // Semaphore status bolts
    String SEMAPHORE_STATUS_BOLT = "semaphore_status_bolt";
    String SEMAPHORE_STATUS_PUBLISHER_BOLT = "semaphore_status_publisher_bolt";
    String REMOVE_STREAM = "remove_stream";
    String UPDATE_STREAM = "update_stream";

    // Ranking Bolts
    String MEAN_CALCULATOR_BOLT = "mean_calculator_bolt";
    String PARTIAL_RANK_BOLT = "partial_rank_bolt";
    String GLOBAL_RANK_BOLT = "global_rank_bolt";
    String RANKABLE_OBJECT = "rankable_object";

    // per seconda query
    String INTERSECTION_AND_GLOBAL_MEDIAN = "intersection_and_global_median";
    String FINAL_COMPARATOR = "final_comparator";
    String FIELDS_SELECTION_FOR_MEDIAN = "fields_selector_for_median";
    String REMOVE_GLOBAL_MEDIAN_ITEM = "remove_global_median_item";
    String UPDATE_GLOBAL_MEDIAN = "update_global_median";
    String VALUE_GLOBAL_MEDIAN = "value_global_median";

    String PARSER_BOLT = "parser";
    String SELECTOR_BOLT = "selector";
    String SELECTOR_BOLT_2 = "selector2";
    String SELECTOR_BOLT_3_4 = "selector3";
    String SHEDDER = "shedder";
    String FILTER_BOLT_QUERY_1 = "filter1";
    String FILTER_BOLT_QUERY_2 = "filter2";
    String METRONOME = "metronome";
    String COMPUTE_MEAN_LAMP = "cml";
    String COMPUTE_MEAN_LAMP_24H = "cml24";
    String COMPUTE_MEAN_LAMP_WEEK = "cmlweek";
    String COMPUTE_MEAN_STREET = "cms";
    String COMPUTE_MEAN_STREET_24H = "cms24";
    String COMPUTE_MEAN_STREET_WEEK = "cmsweek";
    String COMPUTE_MEAN_CITY = "cmc";
    String COMPUTE_MEAN_CITY_24H = "cmc24";
    String COMPUTE_MEAN_CITY_WEEK = "cmcweek";
    String COUNT_LAMP_FOR_ROAD = "clfr";
    String LAMP_PQUANTILE = "lpq";
    String GLOBAL_PQUANTILE = "gpq";
    String COMPARATOR = "comp";

    String S_METRONOME = "sMetronome";
    String D_METRONOME = "dMetronome";
    String UPDATE_PARTIAL = "update";
    String REMOVE = "remove";
    String S_TUPLE = "sTuple";
    String S_NUM = "nRoad";
    String MEDIAN = "median";
    String LAMPS_FOR_ROAD = "lampsForRoad";

}
