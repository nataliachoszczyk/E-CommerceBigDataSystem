"""
Zarządza długo działającym procesem Spark Streaming z integracją HBase.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator, BranchPythonOperator
import logging
import os
import subprocess

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'bigdatacombains',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

REALTIME_ANALYTICS_PID_FILE = "/tmp/realtime_analytics.pid"
REALTIME_ANALYTICS_LOG = "/home/win10/airflow/logs/realtime_analytics.log"
VIRTUALENV_PATH = "/home/win10/airflow-env"

def check_if_should_run(**context):
    """Sprawdza, czy streaming analytics powinien działać."""
    try:
        result = subprocess.run(
            ['pgrep', '-f', 'simulator.py'],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            logger.info("Symulator Kafki działa - uruchamiam analytics")
            return True
        else:
            logger.info("Brak aktywnego symulatora - pomijam uruchomienie")
            return False
    except Exception as e:
        logger.error(f"Błąd sprawdzania symulatora: {e}")
        return False

def check_if_running(**context):
    """Sprawdza czy Spark Streaming już działa i zwraca odpowiedni task."""
    if os.path.exists(REALTIME_ANALYTICS_PID_FILE):
        with open(REALTIME_ANALYTICS_PID_FILE, 'r') as f:
            pid = f.read().strip()
        
        try:
            result = subprocess.run(['ps', '-p', pid], capture_output=True)
            if result.returncode == 0:
                logger.info(f"Spark Streaming już działa (PID: {pid})")
                return 'monitor_health'
            else:
                logger.info(f"PID file istnieje ale proces nie działa - uruchamiam")
                os.remove(REALTIME_ANALYTICS_PID_FILE)
                return 'start_streaming_analytics'
        except:
            return 'start_streaming_analytics'
    else:
        logger.info("Spark Streaming nie działa - uruchamiam")
        return 'start_streaming_analytics'

dag = DAG(
    'realtime_analytics_manager',
    default_args=default_args,
    description='Zarządza procesem Spark Streaming z HBase',
    schedule_interval='*/10 * * * *',  
    start_date=datetime(2025, 12, 27, 0, 0),
    catchup=False,
    max_active_runs=1,
    tags=['spark', 'streaming', 'realtime', 'hbase'],
)

should_run = ShortCircuitOperator(
    task_id='check_if_should_run',
    python_callable=check_if_should_run,
    dag=dag,
)

verify_hbase = BashOperator(
    task_id='verify_hbase',
    bash_command=f"""
    echo "=== Weryfikacja HBase ==="
    
    if ! netstat -tuln | grep -q ':9090 '; then
        echo "HBase Thrift Server nie nasłuchuje!"
        exit 1
    fi
    
    echo "Thrift Server OK"
    source {VIRTUALENV_PATH}/bin/activate
    
    python3 -c "
import happybase
conn = happybase.Connection('localhost', 9090)
conn.open()
tables = [t.decode('utf-8') for t in conn.tables()]
if 'realtime_stats' not in tables:
    print('Tabela realtime_stats nie istnieje!')
    exit(1)
print('HBase OK')
conn.close()
"
    """,
    dag=dag,
)

check_running = BranchPythonOperator(
    task_id='check_if_running',
    python_callable=check_if_running,
    dag=dag,
)

start_streaming = BashOperator(
    task_id='start_streaming_analytics',
    bash_command=f"""
    echo "Uruchamiam Spark Streaming..."
    
    mkdir -p /home/win10/spark-checkpoints/realtime
    mkdir -p $(dirname {REALTIME_ANALYTICS_LOG})
    
    cd /home/win10/scripts
    source {VIRTUALENV_PATH}/bin/activate
    
    nohup spark-submit \
        --master local[4] \
        --driver-memory 4g \
        --executor-memory 2g \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
        realtime_analytics.py \
        > {REALTIME_ANALYTICS_LOG} 2>&1 &
    
    echo $! > {REALTIME_ANALYTICS_PID_FILE}
    echo "Uruchomiono (PID: $!)"
    
    sleep 15
    
    if ps -p $! > /dev/null; then
        echo "Proces działa"
        head -20 {REALTIME_ANALYTICS_LOG}
    else
        echo "Proces się nie uruchomił!"
        cat {REALTIME_ANALYTICS_LOG}
        exit 1
    fi
    """,
    dag=dag,
)

monitor_health = BashOperator(
    task_id='monitor_health',
    bash_command=f"""
    echo "=== Monitoring ==="
    
    if [ -f {REALTIME_ANALYTICS_LOG} ]; then
        echo "Ostatnie logi:"
        tail -n 10 {REALTIME_ANALYTICS_LOG}
        
        HBASE_WRITES=$(grep -c "Zapisano.*HBase" {REALTIME_ANALYTICS_LOG} || echo 0)
        echo "Wykryto $HBASE_WRITES zapisów do HBase"
    fi
    """,
    dag=dag,
    trigger_rule='none_failed_min_one_success',
)

should_run >> verify_hbase >> check_running
check_running >> [start_streaming, monitor_health]
start_streaming >> monitor_health
