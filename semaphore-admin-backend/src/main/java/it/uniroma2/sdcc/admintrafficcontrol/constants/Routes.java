package it.uniroma2.sdcc.admintrafficcontrol.constants;

public interface Routes {

    interface SEMAPHORE {
        // Base route
        String SEMAPHORE_BASE_ROUTE = Entities.SEMAPHORE;

        // Routes
        String CREATE = "create/{intersectionId}/{semaphoreId}";
        String GET_SEMAPHORES = "all";
        String GET_SEMAPHORE = "{id}";
        String DELETE_SEMAPHORE= "delete/{id}";
        String UPDATE_SEMAPHORE = "update/{id}";
        String EXIST_SEMAPHORE = "exist/{id}";
    }

    interface ADMIN {
        // Base route
        String ADMIN_BASE_ROUTE = Entities.ADMIN;

        // Routes

        String SIGN_IN = "sign_in";
        String GET_ADMINS = "all";
        String GET_ADMIN = "{id}";
        String DELETE_ADMIN = "delete/{id}";
        String UPDATE_ADMIN = "update/{id}";
    }
}
