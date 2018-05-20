package it.uniroma2.sdcc.trafficcontrol.RESTfulAPI;

import it.uniroma2.sdcc.trafficcontrol.constants.RESTfulServices;
import it.uniroma2.sdcc.trafficcontrol.exceptions.RecordNotFound;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;

import static org.apache.http.protocol.HTTP.USER_AGENT;

public class RESTfulAPI {

    public final static int STATUS_CODE_200 = 200;
    public final static int STATUS_CODE_300 = 300;

    private final static HttpClient client = HttpClientBuilder.create().build();

    public RESTfulAPI() {

    }

    public static void semaphoreExist(Long id) throws IOException, RecordNotFound {
        // TODO A SCOPO DI TEST
        if (id >= 0)
            return;
        // TODO END TEST

        HttpGet request = new HttpGet(String.format(
                RESTfulServices.GET_SEMAPHORE_ID,
                id
        ));

        // Add request header
        request.addHeader("User-Agent", USER_AGENT);
        HttpResponse response = client.execute(request);

        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode < STATUS_CODE_200 || statusCode >= STATUS_CODE_300) {
            throw new RecordNotFound(String.format(
                    "Semaphore with ID: %d not found.",
                    id
            ));
        }

        /*
        System.out.println("Response Code : "
                + response.getStatusLine().getStatusCode());

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
        try {
            RESTfulAPI.semaphoreExist((long) 7);
        } catch (IOException | RecordNotFound e) {
            e.printStackTrace();
        }
    }

}
