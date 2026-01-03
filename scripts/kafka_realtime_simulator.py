#!/usr/bin/env python3

import csv
import json
import time
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError
import sys
import os
from collections import defaultdict
import pytz
import threading
import queue
import itertools

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/home/win10/airflow/logs/kafka_realtime_simulator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class RealtimeEcommerceSimulator:
    def __init__(self, csv_path, kafka_bootstrap_servers='localhost:9092', topic='user-events', target_date=None):
        """
        Inicjalizacja symulatora z mapowaniem grudnia
        """
        self.csv_path = csv_path
        self.topic = topic
        self.target_date = target_date
        self.event_queue = queue.PriorityQueue()
        self.stop_flag = threading.Event()
        self.counter = itertools.count()
        
        self.stats = {
            'sent': 0,
            'failed': 0,
            'skipped': 0,
            'loaded': 0,
            'start_time': datetime.now(),
            'events_by_hour': defaultdict(int)
        }
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                compression_type='gzip',
                batch_size=16384,
                linger_ms=10,
                buffer_memory=67108864
            )
            logger.info(f"Połączono z Kafka: {kafka_bootstrap_servers}")
        except Exception as e:
            logger.error(f"Błąd połączenia z Kafka: {e}")
            raise
    
    def map_december_to_current(self, event_time_str):
        """
        Mapuje czas z grudnia 2019 na grudzień 2025 / styczeń 2026

        Args:
            event_time_str: String z czasem eventu "2019-12-DD HH:MM:SS UTC"
        Returns:
            Zmapowany czas jako datetime
        """
        try:
            event_time = datetime.strptime(event_time_str.replace(' UTC', ''), '%Y-%m-%d %H:%M:%S')
            
            day = event_time.day
            hour = event_time.hour
            minute = event_time.minute
            second = event_time.second
            
            # Mapowanie dni:
            # Dni 27-31 grudnia 2019 -> 27-31 grudnia 2025
            # Dni 1-26 grudnia 2019 -> 1-26 stycznia 2026
            
            current_year = datetime.now().year
            
            if day >= 27:
                mapped_date = datetime(current_year, 12, day, hour, minute, second)
            else:
                mapped_date = datetime(current_year, 1, day, hour, minute, second)
            
            return mapped_date
            
        except Exception as e:
            logger.error(f"Błąd mapowania czasu {event_time_str}: {e}")
            return None
    
    def should_send_now(self, mapped_time):
        """
        Sprawdza czy event powinien być już wysłany
        
        Args:
            mapped_time: Zmapowany czas eventu
        Returns:
            True jeśli event powinien być wysłany
        """
        now = datetime.now()
        return mapped_time <= now
    
    def load_events_for_today(self):
        """
        Ładuje eventy które mają być odtworzone dzisiaj
        """
        logger.info(f"Ładuję eventy z pliku: {self.csv_path}")
        
        today = self.target_date if self.target_date else datetime.now()
        today_day = today.day
        today_month = today.month

        logger.info(f"Data symulacji: {today.strftime('%Y-%m-%d')}")
        
        if today_month == 12:
            target_days = [today_day]
        elif today_month == 1:
            target_days = [today_day]
        else:
            logger.warning("Symulator działa tylko dla grudnia i stycznia!")
            return 0
        
        loaded_count = 0
        
        try:
            with open(self.csv_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                
                for row in reader:
                    event_time_str = row['event_time']
                    
                    if not event_time_str.startswith('2019-12'):
                        continue
                    
                    event_day = int(event_time_str[8:10])
                    
                    should_load = False
                    
                    if today_month == 12 and today_day >= 27:
                        should_load = (event_day == today_day)
                    elif today_month == 1:
                        should_load = (event_day == today_day)
                    elif today_month == 12:
                        should_load = False
                    
                    if should_load:
                        mapped_time = self.map_december_to_current(event_time_str)
                        if mapped_time:
                            event_data = self.process_event(row)
                            event_data['original_time'] = event_time_str
                            event_data['mapped_time'] = mapped_time.isoformat()
                            
                            priority = mapped_time.timestamp()
                            count = next(self.counter)
                            self.event_queue.put((priority, count, event_data))
                            loaded_count += 1
                            
                            if loaded_count % 10000 == 0:
                                logger.info(f"Załadowano {loaded_count} eventów...")
            
        except FileNotFoundError:
            logger.error(f"Nie znaleziono pliku: {self.csv_path}")
            raise
        except Exception as e:
            logger.error(f"Błąd podczas ładowania eventów: {e}")
            raise
        
        self.stats['loaded'] = loaded_count
        logger.info(f"Załadowano {loaded_count} eventów na dziś")
        return loaded_count
    
    def process_event(self, row):
        """
        Przetwarza wiersz CSV na event JSON
        """
        event = {
            'event_time': row['event_time'],
            'event_type': row['event_type'],
            'product_id': int(row['product_id']) if row['product_id'] else None,
            'category_id': row['category_id'],
            'category_code': row['category_code'] if row['category_code'] != '' and row['category_code'] != 'NaN' else None,
            'brand': row['brand'] if row['brand'] != '' and row['brand'] != 'NaN' else None,
            'price': float(row['price']) if row['price'] else 0.0,
            'user_id': int(row['user_id']) if row['user_id'] else None,
            'user_session': row['user_session']
        }
        return event
    
    def send_to_kafka(self, event):
        """
        Wysyła event do Kafka
        """
        try:
            event['processing_time'] = datetime.now().isoformat()
            
            # Używamy user_id jako klucza partycji
            key = str(event['user_id']).encode('utf-8') if event['user_id'] else None
            
            future = self.producer.send(
                self.topic,
                value=event,
                key=key
            )
            
            self.stats['sent'] += 1
            
            hour = datetime.now().hour
            self.stats['events_by_hour'][hour] += 1
            
            if self.stats['sent'] % 1000 == 0:
                self.log_progress()
            
            return True
            
        except KafkaError as e:
            logger.error(f"Błąd wysyłania do Kafka: {e}")
            self.stats['failed'] += 1
            return False
    
    def process_queue(self):
        """
        Przetwarza kolejkę eventów w czasie rzeczywistym
        """
        logger.info("Rozpoczynam przetwarzanie kolejki eventów...")
        
        batch = []
        batch_size = 100
        last_flush = time.time()
        idle_count = 0
        max_idle_iterations = 300 
        
        while not self.stop_flag.is_set() and not self.event_queue.empty():
            try:
                priority, count, event = self.event_queue.get(timeout=1)
                
                idle_count = 0
                
                mapped_time = datetime.fromisoformat(event['mapped_time'])
                
                if self.should_send_now(mapped_time):
                    self.send_to_kafka(event)
                else:
                    wait_seconds = (mapped_time - datetime.now()).total_seconds()
                    
                    if wait_seconds > 0 and wait_seconds < 3600:
                        logger.debug(f"Czekam {wait_seconds:.1f}s na event z {mapped_time}")
                        time.sleep(wait_seconds)
                        self.send_to_kafka(event)
                    else:
                        self.event_queue.put((priority, count, event))
                        time.sleep(1)
                
                if time.time() - last_flush > 10:
                    self.producer.flush()
                    last_flush = time.time()
                    
            except queue.Empty:
                idle_count += 1
                if idle_count >= max_idle_iterations:
                    logger.info(f"Kolejka pusta przez {max_idle_iterations} sekund - kończę pracę")
                    break
                continue
            except Exception as e:
                logger.error(f"Błąd przetwarzania eventu: {e}")
                self.stats['failed'] += 1
        
        logger.info("Zakończono przetwarzanie kolejki eventów")
    
    def log_progress(self):
        """Loguje postęp przetwarzania"""
        elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
        rate = self.stats['sent'] / elapsed if elapsed > 0 else 0
        
        logger.info(
            f"Postęp: wysłano {self.stats['sent']}/{self.stats['loaded']} eventów, "
            f"błędy: {self.stats['failed']}, "
            f"prędkość: {rate:.2f} event/s"
        )
    
    def log_final_stats(self):
        """Loguje końcowe statystyki"""
        elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
        logger.info("=" * 60)
        logger.info("PODSUMOWANIE SYMULACJI:")
        logger.info(f"Czas trwania: {elapsed:.2f} sekund ({elapsed/3600:.2f} godzin)")
        logger.info(f"Załadowano eventów: {self.stats['loaded']}")
        logger.info(f"Wysłano eventów: {self.stats['sent']}")
        logger.info(f"Pominięto: {self.stats['skipped']}")
        logger.info(f"Błędy: {self.stats['failed']}")
        if elapsed > 0:
            logger.info(f"Średnia prędkość: {self.stats['sent']/elapsed:.2f} event/s")
        
        logger.info("\nRozkład eventów po godzinach:")
        for hour in sorted(self.stats['events_by_hour'].keys()):
            count = self.stats['events_by_hour'][hour]
            logger.info(f"  Godzina {hour:02d}:00 - {count} eventów")
        
        logger.info("=" * 60)
    
    def run_realtime(self):
        """
        Uruchamia symulator w trybie czasu rzeczywistego
        """
        logger.info("=" * 60)
        logger.info("SYMULATOR CZASU RZECZYWISTEGO")
        logger.info(f"Data: {datetime.now().strftime('%Y-%m-%d')}")
        logger.info(f"Topic Kafka: {self.topic}")
        logger.info("=" * 60)
        
        loaded = self.load_events_for_today()
        
        if loaded == 0:
            logger.warning("Brak eventów do przetworzenia na dziś!")
            return
        
        try:
            self.process_queue()
            
            self.producer.flush()
            
        except KeyboardInterrupt:
            logger.info("Przerwano przez użytkownika")
            self.stop_flag.set()
        except Exception as e:
            logger.error(f"Błąd podczas symulacji: {e}")
            raise
        finally:
            self.close()
            self.log_final_stats()
    
    def close(self):
        """Zamyka połączenie z Kafka"""
        try:
            self.producer.close(timeout=10)
            logger.info("Zamknięto połączenie z Kafka")
        except Exception as e:
            logger.error(f"Błąd zamykania połączenia: {e}")

if __name__ == "__main__":
    csv_path = sys.argv[1] if len(sys.argv) > 1 else '/home/win10/2019-Dec.csv'
    simulation_date = None
    if len(sys.argv) > 2:
        try:
            simulation_date = datetime.strptime(sys.argv[2], '%Y-%m-%d')
            logger.info(f"Używam podanej daty symulacji: {simulation_date.strftime('%Y-%m-%d')}")
        except:
            logger.warning("Nieprawidłowy format daty, używam datetime.now()")

    simulator = RealtimeEcommerceSimulator(csv_path=csv_path, target_date=simulation_date)
    simulator.run_realtime()
