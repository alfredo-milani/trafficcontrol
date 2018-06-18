package it.uniroma2.sdcc.trafficcontrol.constants;


public interface Constants {

    // TODO TO DELETE

    long MILL_IN_MIN = 1000 * 5;
    long MILL_IN_HOUR = MILL_IN_MIN * 60;//1000 * 60 * 60;
    long MILL_IN_DAY = MILL_IN_HOUR * 24;
    int MIN_IN_HOUR = 10;
    int WINDOW_SIZE_HOUR = 1; //Granularità 1h
    int NOT_FOUND = -1;
    int CONSTANT_P_SQUARED = 5;
    double PERCENTILE = 0.5;
    int WINDOW_SIZE_WEEK = 7;
    int WINDOW_SIZE_DAY = 24;
    long MILL_IN_FIVE = 1000 * 5; //ogni 5 minuti
    double MEDIAN = 0.5;
    int PP_CONSTANT = 5;
    int ARRAY_SIZE = 2;
    int INDEX_BRIGHT = 0;
    int INDEX_CELL = 1;
    double MIN_BRIGHT = 20;
    int MAX_SIZE = 50;
    int MAX_UPDATE_TIME = 0;


}
