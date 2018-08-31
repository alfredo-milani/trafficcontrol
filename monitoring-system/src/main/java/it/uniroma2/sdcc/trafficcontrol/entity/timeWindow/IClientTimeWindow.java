package it.uniroma2.sdcc.trafficcontrol.entity.timeWindow;

import java.util.ArrayList;

public interface IClientTimeWindow<T> {

    long getLowerBoundWindow();

    long getUpperBoundWindow();

    ArrayList<T> getNewEvents();

    ArrayList<T> getCurrentEvents();

    ArrayList<T> getExpiredEvents();

}
