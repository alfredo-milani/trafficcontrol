package it.uniroma2.sdcc.trafficcontrol.bolts;

import java.util.ArrayList;

public interface IWindow<T> {

    ArrayList<T> getNewEventsWindow();

    ArrayList<T> getCurrentEventsWindow();

    ArrayList<T> getExpiredEventsWindow();

}
