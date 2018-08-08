package it.uniroma2.sdcc.admintrafficcontrol.controller;

import it.uniroma2.sdcc.admintrafficcontrol.dao.AdminDAO;
import it.uniroma2.sdcc.admintrafficcontrol.entity.Admin;
import it.uniroma2.sdcc.admintrafficcontrol.exceptions.EntityNotFound;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import javax.validation.constraints.NotNull;
import java.util.List;

@Service
public class AdminController {

    private final AdminDAO adminDAO;

    @Autowired
    public AdminController(AdminDAO adminDAO) {
        this.adminDAO = adminDAO;
    }


    @Transactional
    public @NotNull Admin createAdmin(Admin admin) {
        return adminDAO.save(admin);
    }

    public Admin getAdmin(@NotNull Long id) {
        return adminDAO.getOne(id);
    }

    @Transactional
    public boolean removeAdmin(@NotNull Long id) {
        if (!adminDAO.existsById(id)) {
            return false;
        }

        adminDAO.deleteById(id);
        return true;
    }

    @Transactional
    public Admin updateAdmin(@NotNull Long id, @NotNull Admin newAdmin) throws EntityNotFound {
        Admin adminToUpdate = adminDAO.getOne(id);
        if (adminToUpdate == null)
            throw new EntityNotFound();

        adminToUpdate.update(newAdmin);

        return adminDAO.save(adminToUpdate);
    }

    public List<Admin> getAdmins() {
        return adminDAO.findAll();
    }

    public Admin login(Admin admin) {
        Admin adminDB = adminDAO.findByUsername(admin.getUsername());

        if (adminDB != null) {
            if (adminDB.getPassword().equals(admin.getPassword())) {
                adminDB.setLogin(true);
                return adminDB;
            }
        }

        return null;
    }

}
