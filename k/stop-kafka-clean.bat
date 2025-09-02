@echo off
REM Kafka'yı durdur
call bin\windows\kafka-server-stop.bat

REM Zookeeper node'u sil
call bin\windows\zookeeper-shell.bat localhost:2182 -zk-tls-config-file config\zookeeper-client.properties delete /brokers/ids/0

echo Kafka durduruldu ve Zookeeper node silindi.
pause
