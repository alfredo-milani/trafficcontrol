app.controller('ctrlTrafficLights',['$scope','myService','$sessionStorage','$location', '$uibModal','$log','$mdDialog','myAjax','$route',function($scope,myService,$sessionStorage,$location,$uibModal, $log,$mdDialog,myAjax, $route) {



    var idTrafficLight;

    $scope.ticket = null;
    $scope.relTicket=null;
    $scope.relationName=null;


    $scope.showDetails = function(tipo,param) {
        idTrafficLight = param;
        myService.dataObj = {"id": idTrafficLight};
        if (tipo === 2) {


            $mdDialog.show({
                controller: "DialogController",
                templateUrl: 'html/dialog1.tmpl.html',
                parent: angular.element(document.body),
                clickOutsideToClose: true

            })

        } else if (tipo === 1) {

            $mdDialog.show({
                controller: "DialogController",
                templateUrl: 'html/modalModifyTrafficLight.html',
                parent: angular.element(document.body),
                clickOutsideToClose: true

            })
        }
    };



    $scope.createTrafficLight=function () {



        var init = function () {
            var param = {
                intersectionId: $scope.intersectionId,
                semaphoreId : $scope.semaphoreId,
                semaphoreLatitude : $scope.latitude,
                semaphoreLongitude : $scope.longitude,
                greenLightDuration : $scope.greenDuration

            };
            myAjax.insertTrafficLight(param).then(function (response) {

                //$scope.items = data;
                if (response.status === 201)
                    $location.path("/showAllTrafficLights");

            }, function () {


                $mdDialog.show(
                    $mdDialog.alert()
                        .parent(angular.element(document.querySelector('#popupContainer')))
                        .clickOutsideToClose(true)
                        .title('Operation failed')
                        .textContent("It's not possible to perform operation now")
                        .ariaLabel('Alert Dialog Demo')
                        .ok('Ok')
                        .targetEvent()
                );
            });
        };

        init();



    };

    $scope.deleteTrafficLight=function (par) {



        var init = function () {
            var param = {
                id: par
            };
            myAjax.removeTrafficLight(param, par).then(function (response) {

                //$scope.items = data;
                if (response.status === 200) {
                    console.log("sono nella delete");
                    $route.reload();

                }
            }, function () {


                $mdDialog.show(
                    $mdDialog.alert()
                        .parent(angular.element(document.querySelector('#popupContainer')))
                        .clickOutsideToClose(true)
                        .title('Operation failed')
                        .textContent("It's not possible to perform operation now")
                        .ariaLabel('Alert Dialog Demo')
                        .ok('Ok')
                        .targetEvent()
                );
            });
        };

        init();



    };

    $scope.showAllTrafficLights= function () {

        var param = {};

        var init = function () {

            myAjax.getTrafficLights(param).then(function (response) {

                if (response.status === 200) {
                    $scope.result = false;

                    $scope.records = response.data;

                    $scope.resultNegative = true;
                }
            }, function () {

                $scope.resultNegative=false;

                $scope.result=true;
            });
        };

        init();


    };

    $scope.showAllTrafficLights();



}]);

