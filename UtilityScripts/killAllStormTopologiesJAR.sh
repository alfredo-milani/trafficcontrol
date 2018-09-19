#!/usr/local/bin/bash
. "./base_aliases.sh"

st kill "ValidationTopology"
st kill "SemaphoreStatusTopology"
st kill "FirstTopology"
st kill "SecondTopology"
st kill "ThirdTopology"
st kill "GreenTimingTopology"