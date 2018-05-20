package it.uniroma2.sdcc.trafficcontrol.RESTfulAPI;

import it.uniroma2.sdcc.trafficcontrol.constants.RESTfulServices;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.http.protocol.HTTP.USER_AGENT;

public class RESTfulAPI {

    private final static String CLASS_NAME = RESTfulAPI.class.getName();
    private final static Logger LOGGER = Logger.getLogger(CLASS_NAME);

    public final static int STATUS_CODE_200 = 200;
    public final static int STATUS_CODE_300 = 300;

    private final static HttpClient client = HttpClientBuilder.create().build();

    public RESTfulAPI() {

    }

    public static boolean semaphoreExist(Long id) {
        // TODO A SCOPO DI TEST
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
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode < STATUS_CODE_200 || statusCode >= STATUS_CODE_300) {
            LOGGER.log(
                    Level.WARNING,
                    String.format(
                            "Semaphore with ID: %d not found.",
                            id
                    )
            );
            return false;
        }

        return true;

        /*
        BufferedReader rd = new BufferedReader(
                new InputStreamReader(response.getEntity().getContent())
        );

        StringBuilder result = new StringBuilder();
        for (String line = ""; line != null; line = rd.readLine()) {
            result.append(line);
        }

        return result.toString();
        */
    }

    public static void main(String[] a) {
        LOGGER.log(
                Level.INFO,
                "Record exist: " + RESTfulAPI.semaphoreExist((long) 7)
        );
    }

}
