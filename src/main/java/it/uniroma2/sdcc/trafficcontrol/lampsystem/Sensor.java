package it.uniroma2.sdcc.trafficcontrol.lampsystem;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;


public class Sensor {
    @JsonProperty(Fields.ID)
    private final Long id;
    @JsonProperty(Fields.ID_INTERSECTION)
    private final Long id_intersection;
    @JsonProperty(Fields.LATITUDE)
    private final Float latitude;
    @JsonProperty(Fields.LONGITUDE)
    private final Float longitude;
    @JsonProperty(Fields.TIMESTAMP)
    private Long timestamp;
    @JsonProperty(Fields.GREEN_DURATION)
    private Long greenDuration;
    @JsonProperty(Fields.NUMBER_VEHICLE)
    private Integer numberVehicle;
    @JsonProperty(Fields.MEAN_SPEED)
    private Float meanSpeed;
    @JsonProperty(Fields.STATE_GREEN)
    private Boolean stateGreen;
    @JsonProperty(Fields.STATE_YELLOW)
    private Boolean stateYellow;
    @JsonProperty(Fields.STATE_RED)
    private Boolean stateRed;

    public Sensor(Long id, Long id_intersection, Float latitude, Float longitude, Long timestamp, Long greenDuration, Integer numberVehicle, Float meanSpeed, Boolean stateGreen, Boolean stateYellow, Boolean stateRed) {
        this.id = id;
        this.id_intersection = id_intersection;
        this.latitude = latitude;
        this.longitude = longitude;
        this.timestamp = timestamp;
        this.greenDuration = greenDuration;
        this.numberVehicle = numberVehicle;
        this.meanSpeed = meanSpeed;
        this.stateGreen = stateGreen;
        this.stateYellow = stateYellow;
        this.stateRed = stateRed;
    }

    public Long getId() {
        return id;
    }

    public Long getId_intersection() {
        return id_intersection;
    }

    public Float getLatitude() {
        return latitude;
    }

    public Float getLongitude() {
        return longitude;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Long getGreenDuration() {
        return greenDuration;
    }

    public void setGreenDuration(Long greenDuration) {
        this.greenDuration = greenDuration;
    }

    public Integer getNumberVehicle() {
        return numberVehicle;
    }

    public void setNumberVehicle(Integer numberVehicle) {
        this.numberVehicle = numberVehicle;
    }

    public Float getMeanSpeed() {
        return meanSpeed;
    }

    public void setMeanSpeed(Float meanSpeed) {
        this.meanSpeed = meanSpeed;
    }

    public Boolean getStateGreen() {
        return stateGreen;
    }

    public void setStateGreen(Boolean stateGreen) {
        this.stateGreen = stateGreen;
    }

    public Boolean getStateYellow() {
        return stateYellow;
    }

    public void setStateYellow(Boolean stateYellow) {
        this.stateYellow = stateYellow;
    }

    public Boolean getStateRed() {
        return stateRed;
    }

    public void setStateRed(Boolean stateRed) {
        this.stateRed = stateRed;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
