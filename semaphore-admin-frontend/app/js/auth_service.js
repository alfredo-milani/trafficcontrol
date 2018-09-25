angular.module('AuthServices', ['ngResource', 'ngStorage'])
    .factory('Auth', function($resource, $rootScope, $sessionStorage,$q,$mdDialog){

        /**
         *  User profile resource
         */

        var Profile;

        var auth = {};

        /**
         *  Saves the current user in the root scope
         *  Call this in the app run() method
         */
        auth.init = function(){
            if (auth.isLoggedIn()){
                $rootScope.user = auth.currentUser();
            }
        };

        auth.login = function(username, password){
            var BASE_PATH = "http://localhost:8200/sdcc-admin";
            var ADMIN = "admin";
            var SIGN_IN = "sign_in";


           // var url = BASE_PATH + "/"+ ADMIN +"/" + SIGN_IN;
            var url = "http://localhost:8200/sdcc-admin/admin/sign_in";

            Profile = $resource(url , {}, {
                login: {
                    method: "POST",
                    isArray : false
                }
            });

             return $q(function(resolve, reject){
                Profile.login({username:username, password:password}).$promise
                    .then(function(data) {
                        $sessionStorage.user = data;
                        $rootScope.user = $sessionStorage.user;
                        resolve();
                    }, function(response) {
                        //password wrong
                        if (response.status === 401) {
                            reject();

                            $mdDialog.show(
                                $mdDialog.alert()
                                    .parent(angular.element(document.querySelector('#popupContainer')))
                                    .clickOutsideToClose(true)
                                    .title('Operation failed')
                                    .textContent("Unathorized")
                                    .ariaLabel('Alert Dialog Demo')
                                    .ok('Ok')
                                    .targetEvent()
                            );
                        }else{
                            $mdDialog.show(
                                $mdDialog.alert()
                                    .parent(angular.element(document.querySelector('#popupContainer')))
                                    .clickOutsideToClose(true)
                                    .title('Operation failed')
                                    .textContent("Internal Error")
                                    .ariaLabel('Alert Dialog Demo')
                                    .ok('Ok')
                                    .targetEvent()
                            );
                        }
                        reject();
                    });

            });
        };


        auth.logout = function() {
            delete $sessionStorage.user;
            delete $rootScope.user;

        };


        auth.checkPermissionForView = function(view) {
            if (!view.requiresAuthentication) {
                return true;
            }

            return userHasPermissionForView(view);


        };


        var userHasPermissionForView = function(view){
            if(!auth.isLoggedIn()){
                return false;
            }

            if(!view.permissions || !view.permissions.length){
                return true;
            }

            return auth.userHasPermission(view.permissions);
        };


        auth.userHasPermission = function(permissions){
            if(!auth.isLoggedIn()){
                return false;
            }

            var found = false;
            angular.forEach(permissions, function(permission, index) {
                if ($sessionStorage.user.role.indexOf(permission) >= 0) {
                    found = true;
                    return;
                }
                /*if ($sessionStorage.user.role.equals(permissions[0])){
                    found = true;
                    return;
            }*/
            });
            return found;
        };


        auth.currentUser = function(){
            return $sessionStorage.user;
        };


        auth.isLoggedIn = function(){
            return $sessionStorage.user != null;
        };


        return auth;

 });