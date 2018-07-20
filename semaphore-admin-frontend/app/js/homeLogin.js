
app.controller('ctrlLogin', function($scope, $location, Auth) {

    $scope.username = "";
    $scope.password = "";
    $scope.failed = false;

    $scope.login = function() {
        Auth.login($scope.username, $scope.password)
            .then(function() {
                $location.path("/home");
            }, function() {
                $scope.failed = true;
            });
    };

});

