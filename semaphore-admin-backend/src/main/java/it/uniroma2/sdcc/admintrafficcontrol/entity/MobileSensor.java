package it.uniroma2.sdcc.admintrafficcontrol.entity;


import it.uniroma2.sdcc.admintrafficcontrol.constants.Tables;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;

@Entity
@Getter
@Setter
@NoArgsConstructor
@Table(name = Tables.T_MOBILE_SENSOR.MOBILE_SENSOR)
public class MobileSensor {

    @Id
    @Column(name = Tables.T_MOBILE_SENSOR.ID)
    private Long mobileId;
    @Column(name = Tables.T_MOBILE_SENSOR.TIMESTAMP)
    private Long mobileTimestampUTC;
    @Column(name = Tables.T_MOBILE_SENSOR.LATITUDE)
    private Double mobileLatitude;
    @Column(name = Tables.T_MOBILE_SENSOR.LONGITUDE)
    private Double mobileLongitude;
    @Column(name = Tables.T_MOBILE_SENSOR.SPEED)
    private Short mobileSpeed;

    public MobileSensor(Long mobileId, Long mobileTimestampUTC, Double mobileLatitude, Double mobileLongitude, Short mobileSpeed) {
        this.mobileId = mobileId;
        this.mobileTimestampUTC = mobileTimestampUTC;
        this.mobileLatitude = mobileLatitude;
        this.mobileLongitude = mobileLongitude;
        this.mobileSpeed = mobileSpeed;
    }



    public void update (@NotNull MobileSensor mobileSensorUpdated ){
        if(mobileSensorUpdated.mobileTimestampUTC != null)
            this.mobileTimestampUTC = mobileSensorUpdated.mobileTimestampUTC;
        if(mobileSensorUpdated.mobileLatitude!= null)
            this.mobileLatitude = mobileSensorUpdated.mobileLatitude;
        if(mobileSensorUpdated.mobileLongitude != null)
            this.mobileLongitude = mobileSensorUpdated.mobileLongitude;
        if(mobileSensorUpdated.mobileSpeed != null)
            this.mobileSpeed = mobileSensorUpdated.mobileSpeed;

    }
}
