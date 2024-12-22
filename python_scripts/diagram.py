import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import psycopg2
from sqlalchemy import create_engine

# Подключение к базе данных PostgreSQL
DB_URI = "postgresql://dwh_2024_s016:uzuD7wVGNo%no%dP@rc1a-dxzd1z2do0u0lig6.mdb.yandexcloud.net:6432/dwh_2024_s016"
engine = create_engine(DB_URI)

# Загрузка данных из представления "marts.fact"
data = pd.read_sql("SELECT * FROM marts.fact", engine)

# Преобразование дат в datetime
data['flight_scheduled_ts'] = pd.to_datetime(data['flight_scheduled_ts'])
data['date'] = data['flight_scheduled_ts'].dt.date

# 1. Количество отменённых и задержанных рейсов по дням
cancelled_flights = data[data['cancelled']].groupby('date').size()
delayed_flights = data[data['dep_delay_min'] > 0].groupby('date').size()

plt.figure(figsize=(10, 6))
plt.plot(cancelled_flights.index, cancelled_flights.values, label='Отменённые рейсы', marker='o')
plt.plot(delayed_flights.index, delayed_flights.values, label='Задержанные рейсы', marker='o')
plt.xlabel('Дата', fontsize=10)
plt.ylabel('Количество рейсов', fontsize=10)
plt.title('Количество отменённых и задержанных рейсов по дням', fontsize=12)
plt.legend(fontsize=10)
plt.grid()
plt.show()

# 2. Процент отменённых рейсов для каждого типа погоды
weather_cancelled = data[data['cancelled']].groupby('weather_type_name').size()
weather_total = data.groupby('weather_type_name').size()
cancellation_rate = (weather_cancelled / weather_total * 100).sort_values(ascending=False)

plt.figure(figsize=(10, 6))
cancellation_rate.plot(kind='bar', color='orange')
plt.xlabel('Тип погоды', fontsize=10)
plt.ylabel('Процент отменённых рейсов', fontsize=10)
plt.title('Процент отменённых рейсов для каждого типа погоды', fontsize=12)
plt.xticks(fontsize=8)
plt.yticks(fontsize=8)
plt.grid(axis='y')
plt.show()

# 3. Количество рейсов по дням
flights_per_day = data.groupby('date').size()

plt.figure(figsize=(10, 6))
plt.plot(flights_per_day.index, flights_per_day.values, marker='o', color='green')
plt.xlabel('Дата', fontsize=10)
plt.ylabel('Количество рейсов', fontsize=10)
plt.title('Количество рейсов по дням', fontsize=12)
plt.xticks(fontsize=8)
plt.yticks(fontsize=8)
plt.grid()
plt.show()

# 4. Средняя дистанция рейсов по дням
average_distance_per_day = data.groupby('date')['distance'].mean()

plt.figure(figsize=(10, 6))
plt.plot(average_distance_per_day.index, average_distance_per_day.values, marker='o', color='purple')
plt.xlabel('Дата', fontsize=10)
plt.ylabel('Средняя дистанция (км)', fontsize=10)
plt.title('Средняя дистанция рейсов по дням', fontsize=12)
plt.xticks(fontsize=8)
plt.yticks(fontsize=8)
plt.grid()
plt.show()

# 5. Распределение задержек по времени суток
data['hour'] = data['flight_scheduled_ts'].dt.hour
hourly_delays = data[data['dep_delay_min'] > 0].groupby('hour')['dep_delay_min'].mean()

plt.figure(figsize=(10, 6))
hourly_delays.plot(kind='bar', color='blue')
plt.xlabel('Час дня', fontsize=10)
plt.ylabel('Средняя задержка (минуты)', fontsize=10)
plt.title('Средняя задержка по времени суток', fontsize=12)
plt.xticks(fontsize=8)
plt.yticks(fontsize=8)
plt.grid(axis='y')
plt.show()

# 6. Распределение расстояний рейсов
plt.figure(figsize=(10, 6))
data['distance'].plot(kind='hist', bins=30, color='skyblue', edgecolor='black')
plt.xlabel('Расстояние (км)', fontsize=10)
plt.ylabel('Частота', fontsize=10)
plt.title('Распределение расстояний рейсов', fontsize=12)
plt.xticks(fontsize=8)
plt.yticks(fontsize=8)
plt.grid(axis='y')
plt.show()

# 7. Процентное распределение типов погоды
weather_distribution = data['weather_type_name'].value_counts(normalize=True) * 100

plt.figure(figsize=(12, 8))
weather_distribution.plot(kind='bar', color='skyblue', edgecolor='black')
plt.xlabel('Тип погоды', fontsize=10)
plt.ylabel('Процент (%)', fontsize=10)
plt.title('Распределение типов погоды', fontsize=12)
plt.xticks(fontsize=8)
plt.yticks(fontsize=8)
plt.grid(axis='y')
plt.show()

# 8. Корреляция между задержкой и расстоянием
plt.figure(figsize=(10, 6))
plt.scatter(data['distance'], data['dep_delay_min'], alpha=0.5, color='red')
plt.xlabel('Расстояние (км)', fontsize=10)
plt.ylabel('Задержка (минуты)', fontsize=10)
plt.title('Корреляция между расстоянием и задержкой', fontsize=12)
plt.xticks(fontsize=8)
plt.yticks(fontsize=8)
plt.grid()
plt.show()

# 9. Средняя задержка по погодным условиям
average_delay_by_weather = data.groupby('weather_type_name')['dep_delay_min'].mean().sort_values(ascending=False)

plt.figure(figsize=(12, 8))
average_delay_by_weather.plot(kind='bar', color='teal', edgecolor='black')
plt.xlabel('Тип погоды', fontsize=10)
plt.ylabel('Средняя задержка (минуты)', fontsize=10)
plt.title('Средняя задержка по погодным условиям', fontsize=12)
plt.xticks(fontsize=8, rotation=45)
plt.yticks(fontsize=8)
plt.grid(axis='y')
plt.show()

# 10. Количество рейсов по часам
flights_by_hour = data.groupby('hour').size()

plt.figure(figsize=(12, 8))
flights_by_hour.plot(kind='bar', color='salmon', edgecolor='black')
plt.xlabel('Час дня', fontsize=10)
plt.ylabel('Количество рейсов', fontsize=10)
plt.title('Количество рейсов по часам', fontsize=12)
plt.xticks(fontsize=8)
plt.yticks(fontsize=8)
plt.grid(axis='y')
plt.show()

# 11. Средняя задержка по дням
average_delay_per_day = data.groupby('date')['dep_delay_min'].mean()

plt.figure(figsize=(12, 6))
plt.plot(average_delay_per_day.index, average_delay_per_day.values, color='blue', marker='o')
plt.xlabel('Дата', fontsize=12)
plt.ylabel('Средняя задержка (минуты)', fontsize=12)
plt.title('Средняя задержка рейсов по дням', fontsize=14)
plt.grid()
plt.show()

# 12. Количество рейсов по авиакомпаниям
flights_by_airline = data['airline'].value_counts()

plt.figure(figsize=(12, 6))
flights_by_airline.plot(kind='bar', color='skyblue', edgecolor='black')
plt.xlabel('Авиакомпания', fontsize=12)
plt.ylabel('Количество рейсов', fontsize=12)
plt.title('Количество рейсов по авиакомпаниям', fontsize=14)
plt.xticks(fontsize=10, rotation=45)
plt.grid(axis='y')
plt.show()

# 13. Средняя задержка по авиакомпаниям
average_delay_by_airline = data.groupby('airline')['dep_delay_min'].mean().sort_values(ascending=False)

plt.figure(figsize=(12, 6))
average_delay_by_airline.plot(kind='bar', color='orange', edgecolor='black')
plt.xlabel('Авиакомпания', fontsize=12)
plt.ylabel('Средняя задержка (минуты)', fontsize=12)
plt.title('Средняя задержка по авиакомпаниям', fontsize=14)
plt.xticks(fontsize=10, rotation=45)
plt.grid(axis='y')
plt.show()

# 14. Распределение задержек по времени суток
data['hour'] = data['flight_scheduled_ts'].dt.hour
hourly_delays = data[data['dep_delay_min'] > 0].groupby('hour')['dep_delay_min'].mean()

plt.figure(figsize=(12, 6))
hourly_delays.plot(kind='bar', color='green', edgecolor='black')
plt.xlabel('Час дня', fontsize=12)
plt.ylabel('Средняя задержка (минуты)', fontsize=12)
plt.title('Средняя задержка по времени суток', fontsize=14)
plt.grid(axis='y')
plt.xticks(fontsize=10)
plt.show()

if 'flight_type' in data.columns:
    flight_types = data['flight_type'].value_counts(normalize=True) * 100

    plt.figure(figsize=(8, 6))
    flight_types.plot(kind='pie', autopct='%1.1f%%', startangle=90, colors=['gold', 'lightblue'])
    plt.ylabel('')
    plt.title('Распределение типов рейсов (внутренние и международные)', fontsize=14)
    plt.show()

# 16. Средняя задержка по типу рейсов (внутренние/международные)
if 'flight_type' in data.columns:
    average_delay_by_flight_type = data.groupby('flight_type')['dep_delay_min'].mean()

    plt.figure(figsize=(12, 6))
    average_delay_by_flight_type.plot(kind='bar', color='purple', edgecolor='black')
    plt.xlabel('Тип рейса', fontsize=12)
    plt.ylabel('Средняя задержка (минуты)', fontsize=12)
    plt.title('Средняя задержка по типу рейсов', fontsize=14)
    plt.xticks(fontsize=10)
    plt.grid(axis='y')
    plt.show()

# 17. Корреляция между расстоянием и задержкой
if 'distance' in data.columns:
    plt.figure(figsize=(12, 6))
    plt.scatter(data['distance'], data['dep_delay_min'], alpha=0.5, color='red')
    plt.xlabel('Расстояние (км)', fontsize=12)
    plt.ylabel('Задержка (минуты)', fontsize=12)
    plt.title('Корреляция между расстоянием и задержкой', fontsize=14)
    plt.grid()
    plt.show()

# 18. Количество рейсов по дням недели
if 'weekday' not in data.columns:
    data['weekday'] = data['flight_scheduled_ts'].dt.day_name()
flights_by_weekday = data['weekday'].value_counts()

plt.figure(figsize=(12, 6))
flights_by_weekday.plot(kind='bar', color='cyan', edgecolor='black')
plt.xlabel('День недели', fontsize=12)
plt.ylabel('Количество рейсов', fontsize=12)
plt.title('Количество рейсов по дням недели', fontsize=14)
plt.xticks(fontsize=10, rotation=45)
plt.grid(axis='y')
plt.show()