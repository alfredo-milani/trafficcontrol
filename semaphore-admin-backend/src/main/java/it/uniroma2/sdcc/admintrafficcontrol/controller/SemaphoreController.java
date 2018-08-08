package it.uniroma2.sdcc.admintrafficcontrol.controller;

import it.uniroma2.sdcc.admintrafficcontrol.dao.SemaphoreDAO;
import it.uniroma2.sdcc.admintrafficcontrol.entity.Semaphore;
import it.uniroma2.sdcc.admintrafficcontrol.exceptions.EntityNotFound;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import javax.validation.constraints.NotNull;
import java.util.List;


@Service
public class SemaphoreController {


    public final SemaphoreDAO semaphoreDAO;

    @Autowired
    public SemaphoreController(SemaphoreDAO semaphoreDAO) {
        this.semaphoreDAO = semaphoreDAO;
    }

    public Semaphore getSemaphore(@NotNull Long id) {
        return semaphoreDAO.getOne(id);
    }

    public Semaphore getSemaphoreByIntersectionAndSemaphoreId(@NotNull Long intersectionId, @NotNull Long semaphoreId) {
        return semaphoreDAO.getByIntersectionIdAndSemaphoreId(intersectionId,semaphoreId);
    }

    @Transactional
    public @NotNull Semaphore createSemaphore(@NotNull Semaphore semaphore) {
        return semaphoreDAO.save(semaphore);
    }
    @Transactional
    public @NotNull Semaphore updateSemaphore( @NotNull Long id, @NotNull Semaphore semaphore) throws EntityNotFound {

        Semaphore semaphoreToUpdate = semaphoreDAO.getOne(id);
        if (semaphoreToUpdate== null)
            throw new EntityNotFound();
        semaphoreToUpdate.update(semaphore);
        return semaphoreDAO.save(semaphoreToUpdate);

    }

    public Semaphore findSemaphore(@NotNull Long id) {
        return semaphoreDAO.getOne(id);
    }

    @Transactional
    public boolean deleteSemaphore(@NotNull Long id){
        if(!semaphoreDAO.existsById(id)){
            return false;
        }
        semaphoreDAO.deleteById(id);
        return true;
    }

    public List<Semaphore> findAllSemaphore(){
        return semaphoreDAO.findAll();
    }
}
