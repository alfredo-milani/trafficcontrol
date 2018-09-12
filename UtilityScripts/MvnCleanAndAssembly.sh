#!/usr/local/bin/bash
. "/Volumes/Data/Projects/Java/--STORM/trafficcontrol/UtilityScripts/base_aliases.sh"

cd "/Volumes/Data/Projects/Java/--STORM/trafficcontrol/monitoring-system"
mvn -Dmaven.test.skip=true clean && mvn -Dmaven.test.skip=true assembly:assembly