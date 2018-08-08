
app.controller('ctrlLogin', function($scope, $location, Auth) {

    $scope.username = "";
    $scope.password = "";
    $scope.failed = false;

    $scope.loginAdmin = function() {
        console.log("sono in loginAdmin");
        Auth.login($scope.username, $scope.password)
            .then(function() {
                $location.path("/homeCustomer");
            }, function() {
                $scope.failed = true;
            });
    };

});

