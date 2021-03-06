package it.uniroma2.sdcc.admintrafficcontrol.rest;

import it.uniroma2.sdcc.admintrafficcontrol.constants.Routes;
import it.uniroma2.sdcc.admintrafficcontrol.controller.SemaphoreController;
import it.uniroma2.sdcc.admintrafficcontrol.entity.Semaphore;
import it.uniroma2.sdcc.admintrafficcontrol.exceptions.EntityNotFound;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping(path = Routes.SEMAPHORE.BASE_ROUTE)
@CrossOrigin
public class SemaphoreRest {

    private final SemaphoreController semaphoreController;

    @Autowired
    public SemaphoreRest(SemaphoreController semaphoreController) {
        this.semaphoreController = semaphoreController;
    }

    @PostMapping(Routes.SEMAPHORE.CREATE)
    public ResponseEntity<Semaphore> getSemaphore(@RequestBody Semaphore semaphore, @PathVariable Long intersectionId,
                                                  @PathVariable Long semaphoreId) {
        Semaphore newSemaphore=null;
        Semaphore semaphoreAlreadyExist = semaphoreController.getSemaphoreByIntersectionAndSemaphoreId(intersectionId,semaphoreId);
        if(semaphoreAlreadyExist == null){
            newSemaphore = semaphoreController.createSemaphore(semaphore);
        }
        return new ResponseEntity<>(newSemaphore, semaphore== null ? HttpStatus.FOUND : HttpStatus.CREATED);
    }

    @GetMapping(Routes.SEMAPHORE.GET_SEMAPHORES)
    public ResponseEntity<List<Semaphore>> getSemaphores() {
        List<Semaphore> semaphores = semaphoreController.findAllSemaphore();
        return new ResponseEntity<>(semaphores, HttpStatus.OK);
    }

    @GetMapping(Routes.SEMAPHORE.EXIST_SEMAPHORE)
    public ResponseEntity<Boolean> existSemaphore(@PathVariable Long id) {
        Boolean b = semaphoreController.existSemaphore(id);
        return new ResponseEntity<>(b, !b ? HttpStatus.NOT_FOUND : HttpStatus.OK);
    }

    @GetMapping(Routes.SEMAPHORE.GET_SEMAPHORE)
    public ResponseEntity<Semaphore> getSemaphore(@PathVariable Long id) {
        Semaphore semaphore = semaphoreController.getSemaphore(id);
        return new ResponseEntity<>(semaphore, semaphore == null ? HttpStatus.NOT_FOUND : HttpStatus.OK);
    }

    @DeleteMapping(Routes.SEMAPHORE.DELETE_SEMAPHORE)
    public ResponseEntity<Semaphore> deleteSemaphore(@PathVariable Long id) {
        if(!semaphoreController.deleteSemaphore(id))
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @PutMapping(Routes.SEMAPHORE.UPDATE_SEMAPHORE)
    public ResponseEntity<Semaphore> updateSemaphore(@PathVariable Long id, @RequestBody Semaphore semaphore) {
        Semaphore semaphoreUpdated;
        try {
            semaphoreUpdated = semaphoreController.updateSemaphore(id, semaphore);
        } catch (EntityNotFound e) {
            return new ResponseEntity<>(semaphore, HttpStatus.NOT_FOUND);
        }
        return new ResponseEntity<>(semaphoreUpdated, HttpStatus.OK);
    }

}
