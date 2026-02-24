import sqlite3
import pandas as pd
import os

# Загружаем данные из CSV
df = pd.read_csv("retail_sales_dataset.csv")
print(f"Загружено {len(df)} записей из CSV")

# Создаем базу данных
db_path = "retail_sales.db"
if os.path.exists(db_path):
    os.remove(db_path)

conn = sqlite3.connect(db_path)

# Сохраняем DataFrame в SQLite таблицу
# pandas автоматически определит типы данных
df.to_sql('retail_sales', conn, if_exists='replace', index=False)

# Проверяем результат
cursor = conn.cursor()
cursor.execute("SELECT name, type FROM pragma_table_info('retail_sales')")
columns = cursor.fetchall()

print("\nСоздана таблица retail_sales с колонками:")
for col in columns:
    print(f"  - {col[0]}: {col[1]}")

cursor.execute("SELECT COUNT(*) FROM retail_sales")
count = cursor.fetchone()[0]
print(f"\nИмпортировано записей: {count}")

# Показываем пример данных
cursor.execute("SELECT * FROM retail_sales LIMIT 3")
samples = cursor.fetchall()
print("\nПримеры данных:")
for sample in samples:
    print(sample)

conn.close()
print(f"\nБаза данных сохранена: {db_path}")