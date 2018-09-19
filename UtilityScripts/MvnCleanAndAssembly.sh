#!/usr/local/bin/bash
. "./base_aliases.sh"

cd "$root_path"
mvn -Dmaven.test.skip=true clean && mvn -Dmaven.test.skip=true assembly:assembly