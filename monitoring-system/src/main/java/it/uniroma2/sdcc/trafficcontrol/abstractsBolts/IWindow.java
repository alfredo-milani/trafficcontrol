package it.uniroma2.sdcc.trafficcontrol.abstractsBolts;

import java.util.ArrayList;

public interface IWindow<T> {

    ArrayList<T> getNewEvents();

    ArrayList<T> getCurrentEvents();

    ArrayList<T> getExpiredEvents();

}
