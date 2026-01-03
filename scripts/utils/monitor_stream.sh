#!/bin/bash

echo "==================================="
echo "MONITOR STRUMIENIA REAL-TIME"
echo "==================================="

echo ""
if ps aux | grep -v grep | grep kafka_realtime_simulator > /dev/null; then
    PID=$(ps aux | grep -v grep | grep kafka_realtime_simulator | awk '{print $2}')
    EVENTS=$(ps aux | grep -v grep | grep kafka_realtime_simulator | grep -oP '\d{4}-\d{2}-\d{2}' | head -1)
    echo "Symulator działa (PID: $PID, data: $EVENTS)"
else
    echo "Symulator nie działa"
fi

# Statystyki Kafka
echo ""
echo "Statystyki Kafka topic 'user-events':"
/opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 \
    --topic user-events

echo ""
echo "Ostatnie 5 eventów:"
timeout 3 /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic user-events \
    --max-messages 5 \
    --property print.timestamp=true \
    --property print.key=true \
    --from-beginning 2>/dev/null | tail -5

echo ""
echo "Ostatnie logi symulatora:"
TODAY=$(date +%Y-%m-%d)
if [ -f "/home/win10/airflow/logs/realtime_simulator_${TODAY}.log" ]; then
    tail -10 "/home/win10/airflow/logs/realtime_simulator_${TODAY}.log"
else
    tail -10 /home/win10/airflow/logs/kafka_realtime_simulator.log
fi

echo ""
echo "==================================="
