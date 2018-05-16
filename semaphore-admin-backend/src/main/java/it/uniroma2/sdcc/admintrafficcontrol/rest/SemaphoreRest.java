package it.uniroma2.sdcc.admintrafficcontrol.rest;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RestController;

@RestController
@CrossOrigin
public class SemaphoreRest {

    /*
    private final AdminController adminController;

    @Autowired
    public SemaphoreRest(AdminController adminController) {
        this.adminController = adminController;
    }


    @RequestMapping(path = "/signin", method = RequestMethod.POST)
    public Admin saveUser(@RequestBody Admin admin) {
        return adminController.signUp(admin);
    }

    @RequestMapping(path = "/user/", method = RequestMethod.GET)
    public List<Admin> getadmins() {
        return adminController.getAdmins();
    }

    @RequestMapping(path = "/user/update/{ID}", method = RequestMethod.PUT)
    public Admin updateUser(@PathVariable Long id, @RequestBody Admin admin) {
        return adminController.updateAdmin(id, admin);
    }

    @RequestMapping(path = "/user/get/{ID}", method = RequestMethod.GET)
    public Admin getUser(@PathVariable Long id) {
        return adminController.getAdmin(id);
    }

    @RequestMapping(path = "/logIn/{username}", method = RequestMethod.GET)
    public Admin verifyUser(@PathVariable String username) {
        return adminController.logIn(username);
    }

    @RequestMapping(path = "/user/{ID}", method = RequestMethod.DELETE)
    public void deleteUser(@PathVariable Long id) {
        adminController.removeAdmin(id);
    }
    */
}
