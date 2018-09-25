app.controller('ctrlCust', function($scope,$http,$location,$sessionStorage,$route,$window,myService, Auth) {

    $scope.makeVisible=false;


    $scope.newTrafficLight = function() {
        $location.path('/createTrafficLight');
    };



    $scope.logout = function () {
        Auth.logout();
        $location.path("/");

    };



});

