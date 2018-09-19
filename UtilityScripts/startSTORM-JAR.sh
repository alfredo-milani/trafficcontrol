#!/usr/local/bin/bash
. "./base_aliases.sh"

cd "$root_path"
st jar target/monitoring-system-1.0-SNAPSHOT-jar-with-dependencies-without-storm.jar it.uniroma2.sdcc.trafficcontrol.TopologyStarter