package it.uniroma2.sdcc.admintrafficcontrol.rest;

import it.uniroma2.sdcc.admintrafficcontrol.constants.Routes;
import it.uniroma2.sdcc.admintrafficcontrol.controller.AdminController;
import it.uniroma2.sdcc.admintrafficcontrol.entity.Admin;
import it.uniroma2.sdcc.admintrafficcontrol.exceptions.EntityNotFound;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;


@RestController
@RequestMapping(path = Routes.ADMIN.BASE_ROUTE)
@CrossOrigin
public class AdminRest {

    private final AdminController adminController;

    @Autowired
    public AdminRest(AdminController adminController) {
        this.adminController = adminController;
    }

    @GetMapping(Routes.ADMIN.GET_ADMINS)
    public ResponseEntity<List<Admin>> getAdmins() {
        List<Admin> persone = adminController.getAdmins();
        return new ResponseEntity<>(persone, HttpStatus.OK);
    }

    @PutMapping(Routes.ADMIN.UPDATE_ADMIN)
    public ResponseEntity<Admin> updateAdmin(@PathVariable Long id, @RequestBody Admin admin) {
        Admin adminUpdated;
        try {
            adminUpdated = adminController.updateAdmin(id, admin);
        } catch (EntityNotFound e) {
            return new ResponseEntity<>(admin, HttpStatus.NOT_FOUND);
        }
        return new ResponseEntity<>(adminUpdated, HttpStatus.OK);
    }

    @GetMapping(Routes.ADMIN.GET_ADMIN)
    public ResponseEntity<Admin> getAdmin(@PathVariable Long id) {
        Admin admin = adminController.getAdmin(id);
        return new ResponseEntity<>(admin, admin == null ? HttpStatus.NOT_FOUND : HttpStatus.CREATED);
    }

    @PostMapping(Routes.ADMIN.SIGN_IN)
    public ResponseEntity<Admin> logIn(@RequestBody Admin admin) {
        Admin adminLogged = adminController.login(admin);
        return new ResponseEntity<>(adminLogged, adminLogged == null ? HttpStatus.UNAUTHORIZED : HttpStatus.OK);
    }

    @DeleteMapping(Routes.ADMIN.DELETE_ADMIN)
    public ResponseEntity<Boolean> deleteAdmin(@PathVariable Long id) {
        boolean deleted = adminController.removeAdmin(id);
        return new ResponseEntity<>(deleted, deleted ? HttpStatus.OK : HttpStatus.NOT_FOUND);
    }

}
