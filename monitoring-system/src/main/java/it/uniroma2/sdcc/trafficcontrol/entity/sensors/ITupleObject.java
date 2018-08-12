package it.uniroma2.sdcc.trafficcontrol.entity.sensors;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

public interface ITupleObject extends Serializable {

    ObjectMapper mapper = new ObjectMapper();

    String getJsonStringFromInstance();

}
