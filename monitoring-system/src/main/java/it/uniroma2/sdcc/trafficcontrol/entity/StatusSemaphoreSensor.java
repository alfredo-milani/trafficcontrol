package it.uniroma2.sdcc.trafficcontrol.entity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.*;

public class StatusSemaphoreSensor implements ISensor {

    public enum SemaphoreStatus {
        WORKING,
        AVERAGE,
        FAULTY
    }

    private final static ObjectMapper mapper = new ObjectMapper();

    private Long intersectionId;
    private Long semaphoreId;
    private Double semaphoreLatitude;
    private Double semaphoreLongitude;
    private Long semaphoreTimestampUTC;
    private SemaphoreStatus greenLightStatus;
    private SemaphoreStatus yellowLightStatus;
    private SemaphoreStatus redLightStatus;

    public StatusSemaphoreSensor(Long intersectionId, Long semaphoreId,
                                 Double semaphoreLatitude, Double semaphoreLongitude,
                                 Long semaphoreTimestampUTC, SemaphoreStatus greenLightStatus,
                                 SemaphoreStatus yellowLightStatus, SemaphoreStatus redLightStatus) {
        this.intersectionId = intersectionId;
        this.semaphoreId = semaphoreId;
        this.semaphoreLatitude = semaphoreLatitude;
        this.semaphoreLongitude = semaphoreLongitude;
        this.semaphoreTimestampUTC = semaphoreTimestampUTC;
        this.greenLightStatus = greenLightStatus;
        this.yellowLightStatus = yellowLightStatus;
        this.redLightStatus = redLightStatus;
    }

    public static StatusSemaphoreSensor getInstanceFrom(RichSemaphoreSensor richSemaphoreSensor) {
        SemaphoreStatus[] semaphoreStates = getStatusFromByte(
                richSemaphoreSensor.getGreenLightStatus(),
                richSemaphoreSensor.getYellowLightStatus(),
                richSemaphoreSensor.getRedLightStatus()
        );

        return new StatusSemaphoreSensor(
                richSemaphoreSensor.getIntersectionId(),
                richSemaphoreSensor.getSemaphoreId(),
                richSemaphoreSensor.getSemaphoreLatitude(),
                richSemaphoreSensor.getSemaphoreLongitude(),
                richSemaphoreSensor.getSemaphoreTimestampUTC(),
                semaphoreStates[0],
                semaphoreStates[1],
                semaphoreStates[2]
        );
    }

    public String getJsonStringFromInstance() {
        ObjectNode objectNode = mapper.createObjectNode();

        objectNode.put(INTERSECTION_ID, intersectionId);
        objectNode.put(SEMAPHORE_ID, semaphoreId);
        objectNode.put(SEMAPHORE_LATITUDE, semaphoreLatitude);
        objectNode.put(SEMAPHORE_LONGITUDE, semaphoreLongitude);
        objectNode.put(SEMAPHORE_TIMESTAMP_UTC, semaphoreTimestampUTC);
        objectNode.put(GREEN_LIGHT_STATUS, greenLightStatus.toString());
        objectNode.put(YELLOW_LIGHT_STATUS, yellowLightStatus.toString());
        objectNode.put(RED_LIGHT_STATUS, redLightStatus.toString());

        return objectNode.toString();
    }

    /**
     * Ritorna un array contenente lo stato di un semaforo
     *
     * @param greenLightStatus  Stato lampada verde
     * @param yellowLightStatus Stato lampada gialla
     * @param redLightStatus    Stato lampada rossa
     * @return Array di {@link SemaphoreStatus} contenente lo stato delle luci di un semaforo,
     * rispettivamente del verde (posizione 0), gialla (posizione 1) e rossa (posizoine 2)
     */
    private static SemaphoreStatus[] getStatusFromByte(Byte greenLightStatus, Byte yellowLightStatus, Byte redLightStatus) {
        SemaphoreStatus[] lightStatus = {
                SemaphoreStatus.WORKING,
                SemaphoreStatus.WORKING,
                SemaphoreStatus.WORKING
        };
        if (greenLightStatus < LAMP_CODE_TWO_THIRD ||
                yellowLightStatus < LAMP_CODE_TWO_THIRD ||
                redLightStatus < LAMP_CODE_OK) {
            if (greenLightStatus >= LAMP_CODE_FAULTY && greenLightStatus < LAMP_CODE_ONE_THIRD) {
                lightStatus[0] = SemaphoreStatus.FAULTY;
            } else if (greenLightStatus >= LAMP_CODE_ONE_THIRD && greenLightStatus < LAMP_CODE_TWO_THIRD) {
                lightStatus[0] = SemaphoreStatus.AVERAGE;
            }

            if (yellowLightStatus >= LAMP_CODE_FAULTY && yellowLightStatus < LAMP_CODE_ONE_THIRD) {
                lightStatus[1] = SemaphoreStatus.FAULTY;
            } else if (yellowLightStatus >= LAMP_CODE_ONE_THIRD && yellowLightStatus < LAMP_CODE_TWO_THIRD) {
                lightStatus[1] = SemaphoreStatus.AVERAGE;
            }

            if (redLightStatus >= LAMP_CODE_FAULTY && redLightStatus < LAMP_CODE_ONE_THIRD) {
                lightStatus[2] = SemaphoreStatus.FAULTY;
            } else if (redLightStatus >= LAMP_CODE_ONE_THIRD && redLightStatus < LAMP_CODE_TWO_THIRD) {
                lightStatus[2] = SemaphoreStatus.AVERAGE;
            }

            /*
            greenStatus = LAMP_STATUS_CODE.get(greenLightStatus) == null ?
                    LAMP_STATUS_OK : LAMP_STATUS_CODE.get(greenLightStatus);
            yellowStatus = LAMP_STATUS_CODE.get(yellowLightStatus) == null ?
                    LAMP_STATUS_OK : LAMP_STATUS_CODE.get(yellowLightStatus);
            redStatus = LAMP_STATUS_CODE.get(redLightStatus) == null ?
                    LAMP_STATUS_OK : LAMP_STATUS_CODE.get(redLightStatus);
            */
        }

        return lightStatus;
    }

    public boolean hasLampsFaulty() {
        return !greenLightStatus.equals(SemaphoreStatus.WORKING) ||
                !yellowLightStatus.equals(SemaphoreStatus.WORKING) ||
                !redLightStatus.equals(SemaphoreStatus.WORKING);
    }

    public Long getIntersectionId() {
        return intersectionId;
    }

    public void setIntersectionId(Long intersectionId) {
        this.intersectionId = intersectionId;
    }

    public Long getSemaphoreId() {
        return semaphoreId;
    }

    public void setSemaphoreId(Long semaphoreId) {
        this.semaphoreId = semaphoreId;
    }

    public Double getSemaphoreLatitude() {
        return semaphoreLatitude;
    }

    public void setSemaphoreLatitude(Double semaphoreLatitude) {
        this.semaphoreLatitude = semaphoreLatitude;
    }

    public Double getSemaphoreLongitude() {
        return semaphoreLongitude;
    }

    public void setSemaphoreLongitude(Double semaphoreLongitude) {
        this.semaphoreLongitude = semaphoreLongitude;
    }

    public Long getSemaphoreTimestampUTC() {
        return semaphoreTimestampUTC;
    }

    public void setSemaphoreTimestampUTC(Long semaphoreTimestampUTC) {
        this.semaphoreTimestampUTC = semaphoreTimestampUTC;
    }

    public SemaphoreStatus getGreenLightStatus() {
        return greenLightStatus;
    }

    public void setGreenLightStatus(SemaphoreStatus greenLightStatus) {
        this.greenLightStatus = greenLightStatus;
    }

    public SemaphoreStatus getYellowLightStatus() {
        return yellowLightStatus;
    }

    public void setYellowLightStatus(SemaphoreStatus yellowLightStatus) {
        this.yellowLightStatus = yellowLightStatus;
    }

    public SemaphoreStatus getRedLightStatus() {
        return redLightStatus;
    }

    public void setRedLightStatus(SemaphoreStatus redLightStatus) {
        this.redLightStatus = redLightStatus;
    }

}
