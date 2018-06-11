package it.uniroma2.sdcc.trafficcontrol.entity;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

public interface ISensor extends Serializable {

    ObjectMapper mapper = new ObjectMapper();

    String getJsonStringFromInstance();

}
