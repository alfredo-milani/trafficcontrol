package it.uniroma2.sdcc.trafficcontrol.entity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.KAFKA_RAW_TUPLE;
import static it.uniroma2.sdcc.trafficcontrol.constants.MobileSensorTuple.*;

public class RichMobileSensor implements ISensor<RichMobileSensor> {

    private Long mobileId;
    private Long mobileTimestampUTC;
    private Double mobileLatitude;
    private Double mobileLongitude;
    private Short mobileSpeed;

    public RichMobileSensor(Long mobileId, Long mobileTimestampUTC,
                            Double mobileLatitude, Double mobileLongitude,
                            Short mobileSpeed) {
        this.mobileId = mobileId;
        this.mobileTimestampUTC = mobileTimestampUTC;
        this.mobileLatitude = mobileLatitude;
        this.mobileLongitude = mobileLongitude;
        this.mobileSpeed = mobileSpeed;
    }

    public static RichMobileSensor getInstanceFrom(Tuple tuple) {
        try {
            String rawTuple = tuple.getStringByField(KAFKA_RAW_TUPLE);
            JsonNode jsonNode = mapper.readTree(rawTuple);

            // Verifica correttezza valori tupla
            Long mobileId = jsonNode.get(MOBILE_ID).asLong();
            Long mobileTimestampUTC = jsonNode.get(MOBILE_TIMESTAMP_UTC).asLong();
            Double mobileLatitude = jsonNode.get(MOBILE_LATITUDE).asDouble();
            Double mobileLongitude = jsonNode.get(MOBILE_LONGITUDE).asDouble();
            Short mobileSpeed = jsonNode.get(MOBILE_SPEED).shortValue();

            return new RichMobileSensor(
                    mobileId,
                    mobileTimestampUTC,
                    mobileLatitude,
                    mobileLongitude,
                    mobileSpeed
            );
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public String getJsonFromInstance() {
        ObjectNode objectNode = mapper.createObjectNode();
        objectNode.put(MOBILE_ID, mobileId);
        objectNode.put(MOBILE_TIMESTAMP_UTC, mobileTimestampUTC);
        objectNode.put(MOBILE_LATITUDE, mobileLatitude);
        objectNode.put(MOBILE_LONGITUDE, mobileLongitude);
        objectNode.put(MOBILE_SPEED, mobileSpeed);

        return objectNode.toString();
    }

    public Long getMobileId() {
        return mobileId;
    }

    public void setMobileId(Long mobileId) {
        this.mobileId = mobileId;
    }

    public Long getMobileTimestampUTC() {
        return mobileTimestampUTC;
    }

    public void setMobileTimestampUTC(Long mobileTimestampUTC) {
        this.mobileTimestampUTC = mobileTimestampUTC;
    }

    public Double getMobileLatitude() {
        return mobileLatitude;
    }

    public void setMobileLatitude(Double mobileLatitude) {
        this.mobileLatitude = mobileLatitude;
    }

    public Double getMobileLongitude() {
        return mobileLongitude;
    }

    public void setMobileLongitude(Double mobileLongitude) {
        this.mobileLongitude = mobileLongitude;
    }

    public Short getMobileSpeed() {
        return mobileSpeed;
    }

    public void setMobileSpeed(Short mobileSpeed) {
        this.mobileSpeed = mobileSpeed;
    }

}
