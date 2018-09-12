#!/usr/local/bin/bash
. "/Volumes/Data/Projects/Java/--STORM/trafficcontrol/UtilityScripts/base_aliases.sh"

base_path="/Volumes/ramdisk/tmp_log/tmp/"
ehcache="ehcache_cache_auth"
kafka_logs="kafka-logs"
zookeeper="zookeeper"

rm -rf "$base_path$ehcache" && echo "Rimossi $base_path$ehcache"
rm -rf "$base_path$kafka_logs" && echo "Rimossi $base_path$kafka_logs"
rm -rf "$base_path$zookeeper" && echo "Rimossi $base_path$zookeeper"
rm -rf "~/.influxdb" "~/.influx_history" && echo "Rimossi ~/.influxdb" "~/.influx_history"
rm -rf "/Volumes/Data/Projects/Java/--STORM/trafficcontrol/logs" && echo "Rimossi /Volumes/Data/Projects/Java/--STORM/trafficcontrol/logs"
rm -rf "/usr/local/apache-storm-1.2.1/logs/ /usr/local/apache-storm-1.2.1/storm-local/" && echo "Rimossi /usr/local/apache-storm-1.2.1/logs/ e /usr/local/apache-storm-1.2.1/storm-local/"