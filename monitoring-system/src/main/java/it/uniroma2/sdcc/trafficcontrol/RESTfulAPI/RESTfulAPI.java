package it.uniroma2.sdcc.trafficcontrol.RESTfulAPI;

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

    public static boolean sensorExistsWithIdFromEndpoint(Long id, String url) {
        // TEST SCOPE
        if (id >= 0) return true;

        HttpGet request = new HttpGet(String.format(url, id));

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
            return false;
        }

        int statusCode = response.getStatusLine().getStatusCode();
        return statusCode >= STATUS_CODE_200 && statusCode < STATUS_CODE_300;
    }

}
