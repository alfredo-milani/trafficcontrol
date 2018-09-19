#!/usr/local/bin/bash
. "./base_aliases.sh"

rm -rf "$base_tmp_path$ehcache" && echo "Rimossi $base_tmp_path$ehcache"
rm -rf "$base_tmp_path$kafka_logs" && echo "Rimossi $base_tmp_path$kafka_logs"
rm -rf "$base_tmp_path$zookeeper" && echo "Rimossi $base_tmp_path$zookeeper"
rm -rf "$influxdb_data" "$influxdb_history" && echo "Rimossi $influxdb_data e $influxdb_history"
rm -rf "$proj_logs" && echo "Rimossi $proj_logs"
rm -rf "$apache_logs" "$apache_local" && echo "Rimossi $apache_logs e $apache_local"