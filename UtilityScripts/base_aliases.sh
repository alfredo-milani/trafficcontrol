# NOTA: se gli scripts si avviano da IntelliJ, è necessario
# inserire come working directory la directory /abs/path/.../UtilityScripts

# per far espandere gli alias in shell non interattive
shopt -s expand_aliases

# monitoring-system root module path
root_path='/Volumes/Data/Projects/Java/--STORM/trafficcontrol/monitoring-system'

# storm
alias st='/usr/local/apache-storm-1.2.1/bin/storm'

# maven
alias mvn='/usr/local/apache-maven-3.5.3/bin/mvn'

# zookeeper
alias zkstart='/usr/local/zookeeper-3.4.10/bin/zkServer.sh start /usr/local/zookeeper-3.4.10/conf/zoo.cfg'
alias zkstop='/usr/local/zookeeper-3.4.10/bin/zkServer.sh stop /usr/local/zookeeper-3.4.10/conf/zoo.cfg'

# kafka
alias kkstart='/usr/local/kafka_2.12-1.0.1/bin/kafka-server-start.sh /usr/local/kafka_2.12-1.0.1/config/server.properties'
alias kkstop='/usr/local/kafka_2.12-1.0.1/bin/kafka-server-stop.sh'
alias kktc='/usr/local/kafka_2.12-1.0.1/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic'
alias kktl='/usr/local/kafka_2.12-1.0.1/bin/kafka-topics.sh --list --zookeeper localhost:2181'
alias kkp='/usr/local/kafka_2.12-1.0.1/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic'
alias kkc='/usr/local/kafka_2.12-1.0.1/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning --topic'

# influxdb
alias xdbc='/usr/local/influxdb-1.6.1-1/usr/bin/influx'
alias xdbd='/usr/local/influxdb-1.6.1-1/usr/bin/influxd'



# path da rimuovere
base_tmp_path='/Volumes/ramdisk/tmp_log/tmp'
ehcache='ehcache_cache_auth'
kafka_logs='kafka-logs'
zookeeper='zookeeper'
influxdb_data='/Users/alfredo/.influxdb'
influxdb_history='/Users/alfredo/.influx_history'
proj_logs='/Volumes/Data/Projects/Java/--STORM/trafficcontrol/logs'
apache_logs='/usr/local/apache-storm-1.2.1/logs/'
apache_local='/usr/local/apache-storm-1.2.1/storm-local/'