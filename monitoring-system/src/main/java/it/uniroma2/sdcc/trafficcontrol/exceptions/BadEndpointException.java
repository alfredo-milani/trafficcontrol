package it.uniroma2.sdcc.trafficcontrol.exceptions;

public class BadEndpointException extends RuntimeException {

    public BadEndpointException() {
        super();
    }

    public BadEndpointException(String message) {
        super(message);
    }

}
