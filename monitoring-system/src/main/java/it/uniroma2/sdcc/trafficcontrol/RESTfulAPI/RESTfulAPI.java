package it.uniroma2.sdcc.trafficcontrol.RESTfulAPI;

import it.uniroma2.sdcc.trafficcontrol.constants.RESTfulServices;
import lombok.extern.java.Log;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;

import static org.apache.http.protocol.HTTP.USER_AGENT;

@Log
public class RESTfulAPI {

    public final static int STATUS_CODE_200 = 200;
    public final static int STATUS_CODE_300 = 300;

    private final static HttpClient client = HttpClientBuilder.create().build();

    public static boolean semaphoreSensorExists(Long id) {
        // TODO A SCOPO DI TEST / SE IL DB è SPENTO
        if (id >= 0)
            return true;
        // TODO END TEST

        HttpGet request = new HttpGet(String.format(
                RESTfulServices.GET_SEMAPHORE_ID,
                id
        ));

        // Add request header
        request.addHeader("User-Agent", USER_AGENT);
        HttpResponse response;
        try {
            response = client.execute(request);
            // This is a crucial step to keep things flowing.
            // We must tell HttpClient that we are done with the connection and that it can now be reused.
            // Without doing this HttpClient will wait indefinitely for a connection to free up so that it can be reused.
            request.releaseConnection();
        } catch (IOException e) {
            log.log(Level.WARNING, e.getMessage());
            return false;
        }

        int statusCode = response.getStatusLine().getStatusCode();
        return statusCode >= STATUS_CODE_200 && statusCode < STATUS_CODE_300;
    }

    public static boolean mobileSensorExists(Long id) {
        // TODO A SCOPO DI TEST / SE IL DB è SPENTO
        if (id >= 0)
            return id >= 0;
        else
            return false;
        // TODO END TEST

        // TODO
    }

    public static void main(String[] a) {
        for (int i = 0; i < 100; ++i) {
            Long random = ThreadLocalRandom.current().nextLong(1, 50);
            log.log(
                    Level.INFO,
                    String.format("ID: %d\tRecord exist: %s", random, RESTfulAPI.semaphoreSensorExists(random))
            );

            /*
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            */
        }
    }

}
