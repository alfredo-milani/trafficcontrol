package it.uniroma2.sdcc.admintrafficcontrol.constants;

public interface Routes {

    interface SEMAPHORE {

        // Base route
        String BASE_ROUTE = Entities.SEMAPHORE;

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
        String BASE_ROUTE = Entities.ADMIN;

        // Routes
        String SIGN_IN = "sign_in";
        String GET_ADMINS = "all";
        String GET_ADMIN = "{id}";
        String DELETE_ADMIN = "delete/{id}";
        String UPDATE_ADMIN = "update/{id}";

    }

    interface MOBILE_SENSOR {

        // Base route
        String BASE_ROUTE = Entities.MOBILE_SENSOR;

        // Routes
        String CREATE = "create/{id}";
        String GET_MOBILE_SENSORS = "all";
        String GET_MOBILE_SENSOR = "{id}";
        String DELETE_MOBILE_SENSOR = "delete/{id}";
        String UPDATE_MOBILE_SENSOR = "update/{id}";
        String EXIST_MOBILE_SENSOR = "exist/{id}";

    }

}
