#!/usr/bin/env python3
"""
Tworzy strukturę tabel HBase dla warstwy serwisowej Lambda Architecture.
"""

import happybase
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

HBASE_HOST = 'localhost'
HBASE_PORT = 9090


def create_hbase_connection():
    """Tworzy połączenie z HBase."""
    try:
        connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
        connection.open()
        logger.info(f"Połączono z HBase na {HBASE_HOST}:{HBASE_PORT}")
        return connection
    except Exception as e:
        logger.error(f"Błąd połączenia z HBase: {e}")
        raise


def create_tables(connection):
    """
    Tworzy wszystkie tabele HBase zgodnie z architekturą projektu.
    """
    tables_config = {
        'realtime_stats': {
            'families': {
                'metrics': dict(max_versions=10), 
                'metadata': dict(max_versions=3) 
            },
            'description': 'Statystyki real-time: aktywni użytkownicy, top produkty, zakupy'
        },
        'funnel_analytics': {
            'families': {
                'stats': dict(max_versions=10),   
                'segments': dict(max_versions=5)  
            },
            'description': 'Analiza lejka zakupowego: view → cart → purchase'
        },
        'personalized_recommendations': {
            'families': {
                'products': dict(max_versions=5), 
                'profile': dict(max_versions=3)   
            },
            'description': 'Rekomendacje według profilu demograficznego'
        },
        'brand_performance': {
            'families': {
                'metrics': dict(max_versions=10),
                'trends': dict(max_versions=5)  
            },
            'description': 'Analiza efektywności marek'
        }
    }
    
    existing_tables = [t.decode('utf-8') for t in connection.tables()]
    
    for table_name, config in tables_config.items():
        if table_name in existing_tables:
            logger.warning(f"Tabela {table_name} już istnieje - pomijam")
            continue
        
        try:
            connection.create_table(
                table_name,
                config['families']
            )
            logger.info(f"  Utworzono tabelę: {table_name}")
            logger.info(f"  Opis: {config['description']}")
            logger.info(f"  Column families: {list(config['families'].keys())}")
        except Exception as e:
            logger.error(f"  Błąd tworzenia tabeli {table_name}: {e}")


def verify_tables(connection):
    """Weryfikuje utworzone tabele."""
    logger.info("\n=== WERYFIKACJA TABEL ===")
    
    tables = [t.decode('utf-8') for t in connection.tables()]
    logger.info(f"Tabele w HBase: {tables}")
    
    for table_name in tables:
        table = connection.table(table_name)
        families = table.families()
        logger.info(f"\nTabela: {table_name}")
        for family, config in families.items():
            logger.info(f"  - {family.decode('utf-8')}: max_versions={config.get(b'MAX_VERSIONS', b'?').decode('utf-8')}")


def insert_test_data(connection):
    """Wstawia testowe dane do weryfikacji działania."""
    logger.info("\n=== TESTOWE DANE ===")
    
    table = connection.table('realtime_stats')
    row_key = f"20251229_2200_active_users".encode('utf-8')
    table.put(row_key, {
        b'metrics:value': b'1523',
        b'metrics:total_events': b'45678',
        b'metadata:window_start': b'2025-12-29 22:00:00',
        b'metadata:window_end': b'2025-12-29 22:05:00',
        b'metadata:created_at': datetime.now().isoformat().encode('utf-8')
    })
    logger.info("✓ Wstawiono testowy rekord do realtime_stats")
    
    row = table.row(row_key)
    logger.info(f"  Odczytano: {row}")


def cleanup_tables(connection, confirm=False):
    """Usuwa wszystkie tabele (OSTROŻNIE!)."""
    if not confirm:
        logger.warning("  Cleanup wymaga potwierdzenia (confirm=True)")
        return
    
    tables_to_delete = ['realtime_stats', 'funnel_analytics', 
                       'personalized_recommendations', 'brand_performance']
    
    for table_name in tables_to_delete:
        try:
            if table_name.encode('utf-8') in connection.tables():
                connection.delete_table(table_name, disable=True)
                logger.info(f"  Usunięto tabelę: {table_name}")
        except Exception as e:
            logger.error(f"  Błąd usuwania {table_name}: {e}")


def main():
    """Główna funkcja setup."""
    logger.info("=== SETUP TABEL HBASE DLA LAMBDA ARCHITECTURE ===\n")
    
    try:
        conn = create_hbase_connection()
        
        logger.info("\n=== TWORZENIE TABEL ===")
        create_tables(conn)
        
        verify_tables(conn)
        
        insert_test_data(conn)
        
        logger.info("\n  Setup zakończony pomyślnie!")
        logger.info("\nNastępne kroki:")
        logger.info("1. Uruchom Spark Streaming z integracją HBase")
        logger.info("2. Monitoruj zapisy: hbase shell → scan 'realtime_stats', {LIMIT => 10}")
        
    except Exception as e:
        logger.error(f"\n  Setup nie powiódł się: {e}")
        raise
    finally:
        try:
            conn.close()
            logger.info("\n  Połączenie z HBase zamknięte")
        except:
            pass


if __name__ == "__main__":
    main()
