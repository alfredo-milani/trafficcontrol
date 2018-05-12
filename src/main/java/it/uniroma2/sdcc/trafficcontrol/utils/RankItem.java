package it.uniroma2.sdcc.trafficcontrol.utils;

import java.io.Serializable;
import java.util.Objects;

public class RankItem implements Serializable {
    private Integer id;
    private String model;
    private Long installationTimestamp;
    private Long meanExpirationTime;
    private Boolean state;

    public RankItem(Integer id, String model, Long installationTimestamp, Long meanExpirationTime, Boolean state) {
        this.id = id;
        this.model = model;
        this.installationTimestamp = installationTimestamp;
        this.meanExpirationTime = meanExpirationTime;
        this.state = state;
    }

    @Override
    public boolean equals(Object obj) {

        if (obj == null || !(obj instanceof RankItem))
            return false;

        RankItem other = (RankItem) obj;

        return Objects.equals(this.id, other.id);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public Long getInstallationTimestamp() {
        return installationTimestamp;
    }

    public void setInstallationTimestamp(Long installationTimestamp) {
        this.installationTimestamp = installationTimestamp;
    }

    public Long getMeanExpirationTime() {
        return meanExpirationTime;
    }

    public void setMeanExpirationTime(Long meanExpirationTime) {
        this.meanExpirationTime = meanExpirationTime;
    }

    public Boolean getState() {
        return state;
    }

    public void setState(Boolean state) {
        this.state = state;
    }
}
