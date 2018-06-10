package it.uniroma2.sdcc.trafficcontrol.entity;

import org.apache.logging.log4j.core.util.Assert;
import org.apache.storm.tuple.Tuple;

public interface RichSensor<T> extends BasicSensor {

    static RichSensor getInstanceFrom(Tuple tuple) {
        Assert.requireNonEmpty(tuple);
        return null;
    }

    String getJsonFromInstance();

}
