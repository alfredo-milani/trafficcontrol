package it.uniroma2.sdcc.admintrafficcontrol.rest;

import it.uniroma2.sdcc.admintrafficcontrol.constants.Routes;
import it.uniroma2.sdcc.admintrafficcontrol.controller.MobileSensorController;
import it.uniroma2.sdcc.admintrafficcontrol.entity.MobileSensor;
import it.uniroma2.sdcc.admintrafficcontrol.entity.Semaphore;
import it.uniroma2.sdcc.admintrafficcontrol.exceptions.EntityNotFound;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping(path = Routes.MOBILE_SENSOR.BASE_ROUTE)
@CrossOrigin
public class MobileSensorRest {

    private final MobileSensorController mobileSensorController;

    @Autowired
    public MobileSensorRest(MobileSensorController mobileSensorController) {
        this.mobileSensorController = mobileSensorController;
    }

    @PostMapping(Routes.MOBILE_SENSOR.CREATE)
    public ResponseEntity<MobileSensor> getMobileSensor(@RequestBody MobileSensor mobileSensor, @PathVariable Long id) {
        MobileSensor newMobileSensor=null;
        if(!mobileSensorController.existMobileSensor(id)){
            newMobileSensor = mobileSensorController.createMobileSensor(mobileSensor);
        }
        return new ResponseEntity<>(newMobileSensor, newMobileSensor== null ? HttpStatus.FOUND : HttpStatus.CREATED);
    }

    @GetMapping(Routes.MOBILE_SENSOR.GET_MOBILE_SENSORS)
    public ResponseEntity<List<MobileSensor>> getMobileSensors() {
        List<MobileSensor> mobileSensors = mobileSensorController.findAllMobileSensor();
        return new ResponseEntity<>(mobileSensors, HttpStatus.OK);
    }

    @GetMapping(Routes.MOBILE_SENSOR.EXIST_MOBILE_SENSOR)
    public ResponseEntity<Boolean> existMobileSensor(@PathVariable Long id) {
        Boolean b = mobileSensorController.existMobileSensor(id);
        return new ResponseEntity<>(b, !b ? HttpStatus.NOT_FOUND : HttpStatus.OK);
    }

    @GetMapping(Routes.MOBILE_SENSOR.GET_MOBILE_SENSOR)
    public ResponseEntity<MobileSensor> getMobileSensor(@PathVariable Long id) {
        MobileSensor mobileSensor = mobileSensorController.getMobileSensor(id);
        return new ResponseEntity<>(mobileSensor, mobileSensor == null ? HttpStatus.NOT_FOUND : HttpStatus.OK);
    }

    @DeleteMapping(Routes.MOBILE_SENSOR.DELETE_MOBILE_SENSOR)
    public ResponseEntity<Semaphore> deleteSemaphore(@PathVariable Long id) {
        if(!mobileSensorController.deleteMobileSensor(id))
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @PutMapping(Routes.MOBILE_SENSOR.UPDATE_MOBILE_SENSOR)
    public ResponseEntity<MobileSensor> updateMobileSensor(@PathVariable Long id, @RequestBody MobileSensor mobileSensor) {
        MobileSensor mobileSensorUpdated;
        try {
            mobileSensorUpdated = mobileSensorController.updateMobileSensor(id, mobileSensor);
        } catch (EntityNotFound e) {
            return new ResponseEntity<>(mobileSensor, HttpStatus.NOT_FOUND);
        }
        return new ResponseEntity<>(mobileSensorUpdated, HttpStatus.OK);
    }

}
