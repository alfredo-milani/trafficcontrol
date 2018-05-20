package it.uniroma2.sdcc.trafficcontrol.constants;

public interface StormParams {

    String KAFKA_SPOUT = "kafkaSpout";
    String VALIDITY_CHECK_BOLT = "validityCheckBolt";
    String AUTHENTICATION_BOLT = "authentication_bolt";
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
    String GLOBAL_RANK = "globalRank";
    String PARTIAL_RANK = "partialRank";


    String S_METRONOME = "sMetronome";
    String D_METRONOME = "dMetronome";
    String UPDATE = "update";
    String REMOVE = "remove";
    String S_TUPLE = "sTuple";
    String S_NUM = "nRoad";
    String MEDIAN = "median";
    String LAMPS_FOR_ROAD = "lampsForRoad";

}
