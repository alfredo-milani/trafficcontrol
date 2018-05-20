package it.uniroma2.sdcc.trafficcontrol.constants;

public interface RESTfulServices {

    String HOSTNAME = "http://localhost:8200";
    String BASE_URL = HOSTNAME + "/sdcc-admin";

    String GET_SEMAPHORE_ID = BASE_URL + "/admin/%d";

}
