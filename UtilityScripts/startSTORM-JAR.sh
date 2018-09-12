#!/usr/local/bin/bash
. "/Volumes/Data/Projects/Java/--STORM/trafficcontrol/UtilityScripts/base_aliases.sh"

cd "/Volumes/Data/Projects/Java/--STORM/trafficcontrol/monitoring-system"
st jar target/monitoring-system-1.0-SNAPSHOT-jar-with-dependencies-without-storm.jar it.uniroma2.sdcc.trafficcontrol.TopologyStarter