"""
DAG Airflow do uruchamiania symulatora w trybie real-time
Uruchamia się raz dziennie o północy i działa przez cały dzień
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import subprocess
import logging

default_args = {
    'owner': 'bigdatacombains',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'realtime_ecommerce_stream',
    default_args=default_args,
    description='Symulacja real-time strumienia danych e-commerce (grudzień 2019 -> grudzień 2025)',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1
)

def check_date_eligibility():
    """Sprawdza czy dzisiejsza data kwalifikuje się do symulacji"""
    today = datetime.now()
    
    # Symulacja działa tylko:
    # - 27-31 grudnia (mapowanie na te same dni grudnia 2019)
    # - 1-31 stycznia (mapowanie na 1-31 grudnia 2019)
    
    if today.month == 12 and today.day >= 27:
        logging.info(f"Symulacja dla {today.day} grudnia")
        return True
    elif today.month == 1:
        logging.info(f"Symulacja dla {today.day} stycznia (dane z {today.day} grudnia 2019)")
        return True
    else:
        logging.warning(f"Brak symulacji dla {today.strftime('%Y-%m-%d')}")
        return False

# Task 1: Sprawdzenie daty
check_date = PythonOperator(
    task_id='check_date_eligibility',
    python_callable=check_date_eligibility,
    dag=dag
)

# Task 2: Sprawdzenie Kafki
check_kafka = BashOperator(
    task_id='check_kafka_health',
    bash_command="""
    if ! pgrep -f kafka.Kafka > /dev/null; then
        echo "Kafka nie działa! Uruchamiam..."
        /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties
        sleep 10
    fi

    /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092 | grep user-events
    """,
    dag=dag
)

# Task 3: Cleanup starych symulatorów
cleanup_old_simulators = BashOperator(
    task_id='cleanup_old_simulators',
    bash_command="""
    echo "Sprawdzam stare symulatory..."
    
    ZOMBIE_PIDS=$(ps aux | grep 'kafka_realtime_simulator.py' | grep -v grep | grep -v '2025-12-2[789]' | grep -v '2025-12-3[01]' | grep -v '2026-01' | awk '{print $2}')
    
    if [ ! -z "$ZOMBIE_PIDS" ]; then
        echo "Znaleziono zombie symulatory: $ZOMBIE_PIDS"
        for PID in $ZOMBIE_PIDS; do
            echo "Zabijam zombie proces: $PID"
            kill $PID 2>/dev/null || true
            sleep 2
            # Jeśli nie zginął, force kill
            if ps -p $PID > /dev/null 2>&1; then
                echo "Force kill: $PID"
                kill -9 $PID 2>/dev/null || true
            fi
        done
    else
        echo "Brak zombie symulatorów"
    fi
    
    find /tmp/realtime_simulator_*.pid -type f -mtime +2 -delete 2>/dev/null || true
    
    echo "Cleanup zakończony"
    """,
    dag=dag
)

# Task 4: Utworzenie/sprawdzenie topiku
ensure_topic = BashOperator(
    task_id='ensure_kafka_topic',
    bash_command="""
    /opt/kafka/bin/kafka-topics.sh --create \
        --if-not-exists \
        --topic user-events \
        --bootstrap-server localhost:9092 \
        --partitions 3 \
        --replication-factor 1 \
        --config retention.ms=604800000 \
        --config compression.type=gzip \
        --config segment.ms=3600000
    
    echo "Topic user-events gotowy"
    """,
    dag=dag
)

# Task 4: Uruchomienie symulatora real-time
run_simulator = BashOperator(
    task_id='run_realtime_simulator',
    bash_command="""
    # INTELIGENTNE WYKRYWANIE DATY SYMULACJI
    # - Scheduled run (o północy): execution_date + 1 day
    # - Manual trigger: dzisiejsza data
    
    EXECUTION_DATE="{{ ds }}"
    CURRENT_DATE=$(date +%Y-%m-%d)
    CURRENT_HOUR=$(date +%H)
    
    echo "Execution date ({{ ds }}): $EXECUTION_DATE"
    echo "Current date: $CURRENT_DATE"
    echo "Current hour: $CURRENT_HOUR"
    
    if [ "$EXECUTION_DATE" == "$CURRENT_DATE" ] && [ "$CURRENT_HOUR" != "00" ]; then
        SIMULATION_DATE="$CURRENT_DATE"
        echo "Wykryto MANUAL TRIGGER - używam dzisiejszej daty: $SIMULATION_DATE"
    else
        SIMULATION_DATE=$(date -d "$EXECUTION_DATE + 1 day" +%Y-%m-%d)
        echo "Wykryto SCHEDULED RUN - używam execution_date + 1: $SIMULATION_DATE"
    fi
    
    PID_FILE=/tmp/realtime_simulator_${SIMULATION_DATE}.pid
    LOG_FILE=/home/win10/airflow/logs/realtime_simulator_${SIMULATION_DATE}.log
    
    echo "Data symulacji: $SIMULATION_DATE"
    echo "PID file: $PID_FILE"
    echo "Log file: $LOG_FILE"

    if [ -f $PID_FILE ]; then
        OLD_PID=$(cat $PID_FILE)
        if ps -p $OLD_PID > /dev/null 2>&1; then
            echo "Symulator dla $SIMULATION_DATE już działa (PID: $OLD_PID)"
            echo "Logi: $LOG_FILE"
            echo "POMIJAM uruchomienie nowego symulatora dla tej daty"
            exit 0
        else
            echo "Stary PID file ($OLD_PID) - proces nie istnieje, czyszczę"
            rm $PID_FILE
        fi
    fi
    
    echo "Aktywne symulatory przed uruchomieniem nowego:"
    ps aux | grep kafka_realtime_simulator | grep -v grep || echo "Brak działających symulatorów"
    echo ""
    
    cd /home/win10/scripts
    
    nohup python3 kafka_realtime_simulator.py /home/win10/2019-Dec.csv $SIMULATION_DATE > $LOG_FILE 2>&1 &
    
    NEW_PID=$!
    echo $NEW_PID > $PID_FILE
    
    echo "   Symulator uruchomiony"
    echo "   Execution date: {{ ds }}"
    echo "   Data symulacji: $SIMULATION_DATE"
    echo "   PID: $NEW_PID"
    echo "   PID file: $PID_FILE"
    echo "   Logi: $LOG_FILE"
    """,
    dag=dag
)

# Task 5: Monitoring
initial_monitor = BashOperator(
    task_id='monitor_initial_progress',
    bash_command="""
    EXECUTION_DATE="{{ ds }}"
    CURRENT_DATE=$(date +%Y-%m-%d)
    CURRENT_HOUR=$(date +%H)
    
    if [ "$EXECUTION_DATE" == "$CURRENT_DATE" ] && [ "$CURRENT_HOUR" != "00" ]; then
        SIMULATION_DATE="$CURRENT_DATE"
    else
        SIMULATION_DATE=$(date -d "$EXECUTION_DATE + 1 day" +%Y-%m-%d)
    fi
    
    PID_FILE=/tmp/realtime_simulator_${SIMULATION_DATE}.pid
    LOG_FILE=/home/win10/airflow/logs/realtime_simulator_${SIMULATION_DATE}.log
    
    echo "Monitoring symulatora dla daty: $SIMULATION_DATE"
    echo "Czekam 30 sekund na start symulatora..."
    sleep 30
    
    if [ -f $PID_FILE ]; then
        PID=$(cat $PID_FILE)
        if ps -p $PID > /dev/null; then
            echo "  Symulator działa (PID: $PID)"
        else
            echo "  BŁĄD: Symulator się zatrzymał!"
            echo "Ostatnie 50 linii logów:"
            tail -n 50 $LOG_FILE
            exit 1
        fi
    else
        echo "  BŁĄD: PID file nie istnieje!"
        exit 1
    fi
    
    echo ""
    echo "Statystyki topiku user-events:"
    /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
        --broker-list localhost:9092 \
        --topic user-events
    
    echo ""
    echo "Ostatnie 20 linii logów symulatora:"
    tail -n 20 $LOG_FILE
    """,
    trigger_rule='all_done',
    dag=dag
)

check_date >> check_kafka >> cleanup_old_simulators >> ensure_topic >> run_simulator >> initial_monitor
