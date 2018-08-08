package it.uniroma2.sdcc.admintrafficcontrol.entity;

import it.uniroma2.sdcc.admintrafficcontrol.constants.Tables;
import javafx.scene.control.Tab;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;
import javax.validation.constraints.NotNull;

@Entity
@Getter
@Setter
@NoArgsConstructor
@Table(name = Tables.T_SEMAPHORE.SEMAPHORE)
public class Semaphore {

    @Id
    @Column(name = Tables.T_SEMAPHORE.ID)
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;
    @Column(name = Tables.T_SEMAPHORE.INTERSECTION_ID)
    private Long intersectionId;
    @Column(name = Tables.T_SEMAPHORE.SEMAPHORE_ID)
    private Long semaphoreId;
    @Column(name = Tables.T_SEMAPHORE.LATITUDE)
    private Double semaphoreLatitude;
    @Column(name = Tables.T_SEMAPHORE.LONGITUDE)
    private Double semaphoreLongitude;
    @Column(name = Tables.T_SEMAPHORE.GREEN_DURATION)
    private Short greenLightDuration;

    public Semaphore(Long intersectionId, Long semaphoreId, Double semaphoreLatitude, Double semaphoreLongitude, Short greenLightDuration) {
        this.intersectionId = intersectionId;
        this.semaphoreId = semaphoreId;
        this.semaphoreLatitude = semaphoreLatitude;
        this.semaphoreLongitude = semaphoreLongitude;
        this.greenLightDuration = greenLightDuration;
    }




    public void update (@NotNull Semaphore semaphoreUpdated ){
        if(semaphoreUpdated.greenLightDuration != null)
            this.greenLightDuration = semaphoreUpdated.greenLightDuration;
        if(semaphoreUpdated.intersectionId != null)
            this.intersectionId = semaphoreUpdated.intersectionId;
        if(semaphoreUpdated.semaphoreId != null)
            this.semaphoreId = semaphoreUpdated.semaphoreId;

    }
}
