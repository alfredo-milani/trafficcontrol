package it.uniroma2.sdcc.trafficcontrol.constants;

public interface RESTfulServices {

    // Base URL
    String HOSTNAME = "http://localhost:8200";
    String BASE_URL = HOSTNAME + "/sdcc_admin";

    // Semaphore resource
    String GET_SEMAPHORE_ID = BASE_URL + "/semaphore/exist/%d";

}
