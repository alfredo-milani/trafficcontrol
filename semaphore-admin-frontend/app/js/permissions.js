angular.module('myApp')
    .directive('permission', ['Auth',function (Auth) {
    return {
        restrict: 'A',
        scope: {
            permission: '='
        },

        link: function (scope,elem,attrs) {
            scope.$watch(Auth.isLoggedIn(),function () {
                if(Auth.userHasPermission(scope.permission)) {
                    console.log("show!!!");
                    elem.show();

                }else{
                    console.log("hide!!!!");
                    elem.hide();
                }
            });
        }

    }
}]);