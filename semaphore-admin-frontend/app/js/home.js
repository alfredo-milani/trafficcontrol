app.controller('ctrlCust', function($scope,$http,$location,$rootScope,myService, Auth,$uibModal,$mdDialog) {

    $scope.makeVisible=false;


    $scope.newTrafficLight = function() {
        $location.path('/createTrafficLight');
    };



    $scope.logout = function () {
        Auth.logout();
        $location.path("/");

    };



});

