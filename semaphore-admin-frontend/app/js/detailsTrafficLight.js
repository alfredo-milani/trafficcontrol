app.controller('DialogController',['$scope','myService','$mdDialog','myAjax','$route',function($scope,myService,$mdDialog,myAjax, $route){


    $scope.hide = function() {
        $mdDialog.hide();
    };

    $scope.cancel = function() {
        $mdDialog.cancel();
    };

    $scope.answer = function(answer) {
        $mdDialog.hide(answer);
    };

    $scope.idTrafLight = myService.dataObj;


    var init = function () {
        var param = {};
        myAjax.getDetailsTrafficLight(param, $scope.idTrafLight.id).then(function (response) {

            if (response.status === 200) {
                $scope.trafficLight= response.data;


            }

        }, function () {

            alert("error in show details");
        });
    };

    init();

    $scope.modifyTL = function () {
        var param = {
            id : $scope.idTrafLight.id,
            intersectionId: $scope.intersectionId,
            semaphoreId : $scope.semaphoreId,
            greenLightDuration : $scope.greenDuration
        };
        myAjax.modifyTrafficLight(param).then(function (response) {

            if (response.status === 200) {
                $scope.trafficLight= response.data;
                $route.reload();


            }

        }, function () {

            alert("error in show details");
        });
    };

    init();




}]);




