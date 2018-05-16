package it.uniroma2.sdcc.admintrafficcontrol.constants;

public interface Routes {

    interface SEMAPHORE {
        // Base route
        String SEMAPHORE_BASE_ROUTE = Entities.SEMAPHORE;

        // Routes
    }

    interface ADMIN {
        // Base route
        String ADMIN_BASE_ROUTE = Entities.ADMIN;

        // Routes
        String SIGN_UP = "sign_up";
        String SIGN_IN = "sign_in";
        String GET_ADMINS = "all";
        String GET_ADMIN = "{id}";
        String DELETE_ADMIN = "delete/{id}";
        String UPDATE_ADMIN = "update/{id}";
    }
}
