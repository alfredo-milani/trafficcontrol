package it.uniroma2.sdcc.trafficcontrol.exceptions;

public class BadTuple extends RuntimeException {

    public BadTuple() {

    }

    public BadTuple(String message) {
        super(message);
    }

}
