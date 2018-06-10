package it.uniroma2.sdcc.trafficcontrol.constants;

import java.util.HashMap;

public interface SemaphoreSensorTuple {

    // Semaphore sensor
    String SEMAPHORE_SENSOR = "semaphore_sensor";

    // Frequenza emissione tuple
    Short SEMAPHORE_EMIT_FREQUENCY = 60;

    // Numero di semafori da attendere prima di computare la media
    Short SEMAPHORE_NUMBER_TO_COMPUTE_SPEED_MEAN = 4;

    // Chiavi per la tupla inviata dai sensori associati ai semafori
    String INTERSECTION_ID = "intersection_id";
    String SEMAPHORE_ID = "semaphore_id";
    String SEMAPHORE_LATITUDE = "semaphore_latitude";
    String SEMAPHORE_LONGITUDE = "semaphore_longitude";
    String SEMAPHORE_TIMESTAMP_UTC = "semaphore_timestamp_utc";
    String GREEN_LIGHT_DURATION = "green_light_duration";
    String GREEN_LIGHT_STATUS = "green_light_status";
    String YELLOW_LIGHT_STATUS = "yellow_light_status";
    String RED_LIGHT_STATUS = "red_light_status";
    String VEHICLES = "vehicles";
    String AVERAGE_VEHICLES_SPEED = "average_vehicles_speed";

    // Codici di stato delle lampade di un semaforo
    String SEMAPHORE_STATUS = "semaphore_status";
    Byte LAMP_CODE_FAULTY = 0;
    String LAMP_STATUS_FAULTY = "FAULTY";
    Byte LAMP_CODE_AVERAGE = 64;
    String LAMP_STATUS_AVERAGE = "AVERAGE";
    Byte LAMP_CODE_OK = Byte.MAX_VALUE;
    String LAMP_STATUS_OK = "WORKING";

    Byte LAMP_CODE_ONE_THIRD = (byte) (LAMP_CODE_OK / 3);
    Byte LAMP_CODE_TWO_THIRD = (byte) (LAMP_CODE_OK * 2 / 3);

    HashMap<Byte, String> LAMP_STATUS_CODE = new HashMap<Byte, String>(Byte.MAX_VALUE + 1) {{
        put(LAMP_CODE_FAULTY, LAMP_STATUS_FAULTY);
        put(LAMP_CODE_AVERAGE, LAMP_STATUS_AVERAGE);
        put(LAMP_CODE_OK, LAMP_STATUS_OK);
    }};
}
