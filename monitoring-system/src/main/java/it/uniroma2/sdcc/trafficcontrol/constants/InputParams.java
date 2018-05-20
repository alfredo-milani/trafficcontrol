package it.uniroma2.sdcc.trafficcontrol.constants;

public class InputParams {

    // Nome applicazione
    public final static String APP_NAME = "Monitoring system";

    // Exit statuc
    public final static byte EXIT_SUCCESS = 0;
    public final static byte EXIT_FAILURE = 1;

    // Parametri di input
    public final static String MODE = "m";
    public final static String MODE_LONG = "mode";
    public final static String MODE_LOCAL = "local";
    public final static String MODE_CLUSTER = "cluster";
    public static String MODE_SELECTED = MODE_LOCAL;

    public final static String KAFKA_IP = "h";
    public final static String KAFKA_IP_LONG = "hostname";
    public static String KAFKA_IP_SELECTED = "localhost";

    public final static String KAFKA_PORT = "p";
    public final static String KAFKA_PORT_LONG = "port";
    public static int KAFKA_PORT_SELECTED = 9092;
    public static String KAFKA_IP_PORT = KAFKA_IP_SELECTED + ":" + KAFKA_PORT_SELECTED;

    public final static String NUMBER_WORKERS = "w";
    public final static String NUMBER_WORKERS_LONG = "workers";
    public static int NUMBER_WORKERS_SELECTED = 2;

}
