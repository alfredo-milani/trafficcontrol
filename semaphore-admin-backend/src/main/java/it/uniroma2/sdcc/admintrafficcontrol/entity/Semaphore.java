package it.uniroma2.sdcc.admintrafficcontrol.entity;

import it.uniroma2.sdcc.admintrafficcontrol.constants.Tables;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;

@Entity
@Getter
@Setter
@NoArgsConstructor
@Table(name = Tables.T_SEMAPHORE.SEMAPHORE)
public class Semaphore {

    @Id
    @Column(name = Tables.T_SEMAPHORE.C_ID)
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;


}
