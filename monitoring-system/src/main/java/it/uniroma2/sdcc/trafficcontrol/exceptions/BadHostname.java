package it.uniroma2.sdcc.trafficcontrol.exceptions;

public class BadHostname extends RuntimeException {

    public BadHostname() {
        super();
    }

    public BadHostname(String message) {
        super(message);
    }

}
