package it.uniroma2.sdcc.admintrafficcontrol.dao;


import it.uniroma2.sdcc.admintrafficcontrol.entity.MobileSensor;
import org.springframework.data.jpa.repository.JpaRepository;

public interface MobileSensorDAO extends JpaRepository<MobileSensor, Long> {
}
