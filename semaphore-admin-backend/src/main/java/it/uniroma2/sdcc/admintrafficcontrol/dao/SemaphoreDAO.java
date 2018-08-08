package it.uniroma2.sdcc.admintrafficcontrol.dao;

import it.uniroma2.sdcc.admintrafficcontrol.entity.Semaphore;
import org.springframework.data.jpa.repository.JpaRepository;

public interface SemaphoreDAO extends JpaRepository<Semaphore, Long> {

    public Semaphore getByIntersectionIdAndSemaphoreId(Long intersectionId, Long semaphoreId);

}
