package it.uniroma2.sdcc.trafficcontrol.exceptions;

public class BadTuple extends RuntimeException {

    public BadTuple() {
        super();
    }

    public BadTuple(String message) {
        super(message);
    }

}
