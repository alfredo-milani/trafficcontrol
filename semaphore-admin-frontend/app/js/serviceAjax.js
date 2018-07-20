app.service("myAjax", function($q,$http) {

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
        return ajax("POST", "http://localhost:8200/ticketingsystem/ticket/", data);

    };

    this.getDetailsTrafficLight = function(params) {
        return ajax("GET", "http://localhost:8200/ticketingsystem/ticket", params);
    };


    this.removeTrafficLight = function (data,idTicket) {
        return ajax("GET", "http://localhost:8200/ticketingsystem/ticket/" + idTicket, data);

    };

    this.getTrafficLights = function (data,idTicket) {
        return ajax("GET", "http://localhost:8200/ticketingsystem/ticket/" + idTicket, data);

    };

    this.modifyUser = function (data,idTicket) {
        return ajax("GET", "http://localhost:8200/ticketingsystem/ticket/" + idTicket, data);

    };

});
