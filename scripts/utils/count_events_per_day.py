#!/usr/bin/env python3
import csv
from collections import defaultdict

days = defaultdict(int)

with open('/home/win10/2019-Dec.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row['event_time'].startswith('2019-12'):
            day = int(row['event_time'][8:10])
            days[day] += 1

print("Liczba eventów na dzień w grudniu 2019:")
for day in sorted(days.keys()):
    print(f"  {day:2d} grudnia: {days[day]:>10,} eventów")
