
// Declare app level module which depends on views, and components
var app = angular.module('myApp', ['ngRoute','ngResource','AuthServices','ngAnimate','ui.bootstrap','ngMaterial','ngMessages']);


app.config(function($routeProvider,$mdThemingProvider) {
    $routeProvider
        .when("/", {
            templateUrl: "html/home.html"

        })
        .when("/homeLogin", {
            templateUrl: "html/homeLogin.html",
            controller: "ctrlLogin"

        })
        .when("/homeCustomer", {
            templateUrl: "html/homeCustomer.html",
            controller: "ctrlCust"
            //requiresAuthentication: true

        })
        .when("/createTrafficLight", {
           templateUrl: "html/CreateTrafficLight.html",
           controller: "ctrlTrafficLights",
            requiresAuthentication: true
        })
        .when("/modifyUser",{
            templateUrl: "html/modifyUser.html",
            controller: "ctrlModifyUser",
            requiresAuthentication: true

        })
        .when("/showAllTrafficLights",{
            templateUrl: "html/showAllTrafficLights.html",
            controller: "ctrlTrafficLights",
            requiresAuthentication: true

        });


});



app.run(function ($rootScope, $location, Auth) {
    Auth.init();

    $rootScope.$on('$routeChangeStart', function (event, next) {
        if (!Auth.checkPermissionForView(next)) {
            event.preventDefault();
            $location.path("/homeLogin");
        }
    });
});













