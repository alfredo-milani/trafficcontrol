package it.uniroma2.sdcc.trafficcontrol.RESTfulAPI;

import it.uniroma2.sdcc.trafficcontrol.entity.configuration.AppConfig;
import it.uniroma2.sdcc.trafficcontrol.exceptions.BadHostname;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.io.Serializable;

import static it.uniroma2.sdcc.trafficcontrol.entity.configuration.AppConfig.DEBUG_LEVEL_MOCK_END_POINTS;
import static org.apache.http.protocol.HTTP.*;

public class RESTfulAPI implements Serializable {

    public final static int STATUS_CODE_200 = 200;
    public final static int STATUS_CODE_300 = 300;
    public final static String JAVA_VERSION = "Java/" + System.getProperty("java.version");
    public final static String JAVA_AGENT = java.security.AccessController
            .doPrivileged(new sun.security.action.GetPropertyAction("http.agent"));
    public final static String JAVA_USER_AGENT = JAVA_AGENT == null
            ? JAVA_VERSION
            : JAVA_AGENT + " " + JAVA_VERSION;

    private AppConfig appConfig;
    private final boolean mockEndpoint;
    private final static HttpClient client = HttpClientBuilder.create().build();

    public RESTfulAPI(AppConfig appConfig) {
        this.appConfig = appConfig;
        mockEndpoint = appConfig.getDebugLevel() == DEBUG_LEVEL_MOCK_END_POINTS;
    }

    @SuppressWarnings("Duplicates")
    public boolean sensorExistsWithIdFromEndpoint(@NotNull Long id, @NotNull String url) {
        if (mockEndpoint) return true;

        HttpGet request = new HttpGet(String.format(url, id));
        /*HttpGet request = new HttpGet(new URIBuilder()
                .setScheme("http")
                .setHost("localhost")
                .setPort(8200)
                .setPath("sdcc-admin/semaphore/exist/6")
                .build()
        );*/

        // Add request header
        request.addHeader(CONN_DIRECTIVE, CONN_KEEP_ALIVE);
        request.addHeader(CONTENT_TYPE, "application/json");
        request.addHeader(USER_AGENT, JAVA_USER_AGENT);

        try {
            HttpResponse response = client.execute(request);
            // This is a crucial step to keep things flowing.
            // We must tell HttpClient that we are done with the connection and that it can now be reused.
            // Without doing this HttpClient will wait indefinitely for a connection to free up so that it can be reused.
            request.releaseConnection();

            int statusCode = response.getStatusLine().getStatusCode();
            return statusCode >= STATUS_CODE_200 && statusCode < STATUS_CODE_300;
        } catch (IOException e) {
            return false;
        }
    }

    @SuppressWarnings("Duplicates")
    public boolean hostIsUp(String hostname) {
        if (mockEndpoint) return true;

        HttpGet request = new HttpGet(hostname);

        // Add request header
        request.addHeader(CONN_DIRECTIVE, CONN_KEEP_ALIVE);
        request.addHeader(CONTENT_TYPE, "application/json");
        request.addHeader(USER_AGENT, JAVA_USER_AGENT);

        try {
            HttpResponse response = client.execute(request);
            // This is a crucial step to keep things flowing.
            // We must tell HttpClient that we are done with the connection and that it can now be reused.
            // Without doing this HttpClient will wait indefinitely for a connection to free up so that it can be reused.
            request.releaseConnection();

            int statusCode = response.getStatusLine().getStatusCode();
            return statusCode >= STATUS_CODE_200 && statusCode < STATUS_CODE_300;
        } catch (IOException e) {
            return false;
        }
    }

    public String getHostname(String URI) {
        String[] strings = URI.split("/");
        if (strings.length < 3) {
            throw new BadHostname("URI non valida: " + URI);
        }

        return String.format(
                "%s/%s/%s/%s",
                strings[0],
                strings[1],
                strings[2],
                strings[3]
        );
    }

}
