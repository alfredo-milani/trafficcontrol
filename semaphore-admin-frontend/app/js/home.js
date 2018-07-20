app.controller('ctrlCust', function($scope,$http,$location,$rootScope,myService, Auth,$uibModal,$mdDialog) {

    $scope.makeVisible=false;


    $scope.newTicket = function() {
        $location.path('/createTicket');
    };



    $scope.logout = function () {
        Auth.logout();
        $location.path("/");

    };



});

