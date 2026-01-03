#!/bin/bash

echo "=== TEST SPARK STREAMING ==="
echo

# 1. Sprawdź czy Kafka działa
echo "1. Sprawdzam Kafka..."
if pgrep -f kafka.Kafka > /dev/null; then
    echo "✓ Kafka działa"
else
    echo "✗ Kafka nie działa - uruchamiam..."
    /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties
    sleep 10
fi

# 2. Sprawdź czy symulator działa
echo
echo "2. Sprawdzam symulator..."
if pgrep -f kafka_realtime_simulator > /dev/null; then
    SIMULATOR_PID=$(pgrep -f kafka_realtime_simulator)
    echo "✓ Symulator działa (PID: $SIMULATOR_PID)"
else
    echo "✗ Symulator nie działa"
    echo "   Uruchom go najpierw jeśli chcesz testować real-time analytics"
fi

# 3. Utwórz katalogi
echo
echo "3. Tworzę katalogi..."
mkdir -p /home/win10/spark-checkpoints/realtime
mkdir -p /tmp/realtime_stats
mkdir -p /home/win10/airflow/logs
echo "✓ Katalogi utworzone"

# 4. Test Spark Streaming (180 sekund)
echo
echo "4. Test Spark Streaming (180 sekund)..."
echo "   Uruchamiam realtime_analytics.py..."

cd /home/win10/scripts

# Uruchom w tle
timeout 180s spark-submit \
    --master local[4] \
    --driver-memory 4g \
    --executor-memory 2g \
    --conf spark.streaming.stopGracefullyOnShutdown=true \
    --conf spark.sql.streaming.checkpointLocation=/home/win10/spark-checkpoints/realtime \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    realtime_analytics.py \
    2>&1 | tee /tmp/realtime_test.log &

TEST_PID=$!
echo "   PID: $TEST_PID"

# Poczekaj 60  sekund
sleep 60

# Sprawdź czy działa
if ps -p $TEST_PID > /dev/null 2>&1; then
    echo "✓ Spark Streaming działa!"
    echo
    echo "--- Ostatnie logi ---"
    tail -30 /tmp/realtime_test.log
    
    # Zabij test
    kill $TEST_PID 2>/dev/null
    wait $TEST_PID 2>/dev/null
    
    echo
    echo "✓ Test zakończony sukcesem!"
    echo
    echo "Sprawdź wyniki w /tmp/realtime_stats/"
    ls -lh /tmp/realtime_stats/*/batch_0/ 2>/dev/null || echo "  (jeszcze brak danych)"
else
    echo "✗ Spark Streaming się zatrzymał"
    echo
    echo "--- Logi błędu ---"
    cat /tmp/realtime_test.log
    exit 1
fi

echo
echo "=== GOTOWY DO WDROŻENIA W AIRFLOW ==="
