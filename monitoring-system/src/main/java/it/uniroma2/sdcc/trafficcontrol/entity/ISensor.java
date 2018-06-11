package it.uniroma2.sdcc.trafficcontrol.entity;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.core.util.Assert;
import org.apache.storm.tuple.Tuple;

import java.io.Serializable;

public interface ISensor<T> extends Serializable {

    ObjectMapper mapper = new ObjectMapper();

    static ISensor getInstanceFrom(Tuple tuple) {
        Assert.requireNonEmpty(tuple);
        return null;
    }

    String getJsonFromInstance();

}
