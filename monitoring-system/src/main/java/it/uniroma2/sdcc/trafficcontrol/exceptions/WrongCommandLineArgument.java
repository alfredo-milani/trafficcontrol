package it.uniroma2.sdcc.trafficcontrol.exceptions;

public class WrongCommandLineArgument extends Exception {

    public WrongCommandLineArgument() {
        super();
    }

    public WrongCommandLineArgument(String message) {
        super(message);
    }

}