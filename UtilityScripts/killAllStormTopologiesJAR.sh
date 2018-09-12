#!/usr/local/bin/bash
. "/Volumes/Data/Projects/Java/--STORM/trafficcontrol/UtilityScripts/base_aliases.sh"

st kill "ValidationTopology"
st kill "SemaphoreStatusTopology"
st kill "FirstTopology"
st kill "SecondTopology"
st kill "ThirdTopology"
st kill "GreenTimingTopology"