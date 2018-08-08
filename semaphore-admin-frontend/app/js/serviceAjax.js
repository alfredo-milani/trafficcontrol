app.service("myAjax", function($q,$http) {

    var BASE_PATH = "http://localhost:8200/sdcc-admin";
    var SEMAPHORE = "semaphore";
    var ADMIN = "admin";
/*
    var CREATE = "create/{intersectionId}/{semaphoreId}";
    var ALL = "all";
    var GET= "{id}";
    var  DELETE= "delete/{id}";
    var UPDATE= "update/{id}";
    var SIGN_IN = "sign_in";*/

    var CREATE = "create";
    var ALL = "all";
    var DELETE= "delete";
    var UPDATE= "update";
    var SIGN_IN = "sign_in";

    var ajax = function(method,url,data) {

        var deferred = $q.defer();
        var request = {
            method: method,
            url: url,
            headers: {
                'Content-Type': 'application/json'
            }
        };
        if (method === 'GET') {
            request.params = data;
        } else {
            request.data = data;
        }
        $http(request).then(function(response){
            deferred.resolve(response);
        },function(response){
            deferred.reject({data: response.data, status: response.status});
        });
        return deferred.promise;
    };



    this.insertTrafficLight = function (data) {
        return ajax("POST", BASE_PATH +"/"+ SEMAPHORE + "/" + CREATE + "/" + data.intersectionId + "/" + data.semaphoreId , data);

    };

    this.getDetailsTrafficLight = function(data, idTrafLight) {
        return ajax("GET", BASE_PATH +"/"+ SEMAPHORE + "/" + idTrafLight, data);
    };


    this.removeTrafficLight = function (data, id) {
        return ajax("DELETE", BASE_PATH +"/"+ SEMAPHORE + "/"+ DELETE + "/" + id, data);

    };

    this.getTrafficLights = function (data) {
        return ajax("GET", BASE_PATH +"/"+ SEMAPHORE + "/" + ALL, data);

    };

    this.modifyTrafficLight = function (data) {
        return ajax("PUT", BASE_PATH +"/"+ SEMAPHORE + "/" + UPDATE + "/" + data.id, data);

    };

});
