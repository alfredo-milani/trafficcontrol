package it.uniroma2.sdcc.admintrafficcontrol.constants;

public interface Tables {

    // Semaphore table
    interface T_SEMAPHORE {
        // Table name
        String SEMAPHORE = Entities.SEMAPHORE;

        // Semaphore columns
        String C_ID = "semaphore_id";
    }

    // Admin table
    interface T_ADMIN {
        // Table name
        String ADMIN = Entities.ADMIN;

        // Admin columns
        String C_ID = "admin_id";
        String C_EMAIL = "email";
        String C_PASSWORD = "password";
        String C_USERNAME = "username";
    }

}