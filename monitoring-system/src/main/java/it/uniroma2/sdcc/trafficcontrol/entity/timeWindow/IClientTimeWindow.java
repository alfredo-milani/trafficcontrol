package it.uniroma2.sdcc.trafficcontrol.entity.timeWindow;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;

public interface IClientTimeWindow<T> {

    long getWindowSize();

    long getLowerBoundWindow();

    long getUpperBoundWindow();

    @NotNull ArrayList<T> getNewEvents();

    @NotNull ArrayList<T> getCurrentEvents();

    @NotNull ArrayList<T> getExpiredEvents();

}
