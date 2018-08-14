package it.uniroma2.sdcc.admintrafficcontrol.controller;

import it.uniroma2.sdcc.admintrafficcontrol.dao.MobileSensorDAO;
import it.uniroma2.sdcc.admintrafficcontrol.entity.MobileSensor;
import it.uniroma2.sdcc.admintrafficcontrol.exceptions.EntityNotFound;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import javax.validation.constraints.NotNull;
import java.util.List;

@Service
public class MobileSensorController {

    private final MobileSensorDAO mobileSensorDAO;

    @Autowired
    public MobileSensorController(MobileSensorDAO mobileSensorDAO) {
        this.mobileSensorDAO=mobileSensorDAO;
    }

    public MobileSensor getMobileSensor(@NotNull Long id) {
        return mobileSensorDAO.getOne(id);
    }

    @Transactional
    public @NotNull MobileSensor createMobileSensor(@NotNull MobileSensor mobileSensor) {
        return mobileSensorDAO.save(mobileSensor);
    }
    @Transactional
    public @NotNull MobileSensor updateMobileSensor( @NotNull Long id, @NotNull MobileSensor mobileSensor) throws EntityNotFound {

        MobileSensor mobileSensorToUpdate = mobileSensorDAO.getOne(id);
        if (mobileSensorToUpdate== null)
            throw new EntityNotFound();
        mobileSensorToUpdate.update(mobileSensor);
        return mobileSensorDAO.save(mobileSensorToUpdate);

    }

    @Transactional
    public boolean deleteMobileSensor(@NotNull Long id){
        if(!mobileSensorDAO.existsById(id)){
            return false;
        }
        mobileSensorDAO.deleteById(id);
        return true;
    }

    public List<MobileSensor> findAllMobileSensor(){
        return mobileSensorDAO.findAll();
    }

    public Boolean existMobileSensor(Long id) {
        return mobileSensorDAO.existsById(id);
    }
}
