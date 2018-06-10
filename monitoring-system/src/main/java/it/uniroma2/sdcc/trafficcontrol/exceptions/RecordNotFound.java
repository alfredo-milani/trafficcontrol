package it.uniroma2.sdcc.trafficcontrol.exceptions;

public class RecordNotFound extends RuntimeException {

    public RecordNotFound() {
        super();
    }

    public RecordNotFound(String message) {
        super(message);
    }

}
