# 启动 flume

Linux 

./bin/flume-ng agent -c ./conf/ -f ./conf/log-kafka.properties -n agent -Dflume.root.logger=INFO,console

Windows

bin\flume-ng.cmd agent -n agent -c conf -f conf\log-kafka.properties -property  "flume.root.logger=INFO,console"
