import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from datetime import datetime

# Подключение к базе данных
conn = sqlite3.connect('retail_sales.db')

# 1. SQL запросы с условиями фильтрации
print("="*50)
print("SQL ЗАПРОСЫ С ФИЛЬТРАЦИЕЙ")
print("="*50)

# Запрос 1: Продажи для женщин старше 50 лет
query1 = """
SELECT customer_id, age, gender, product_category, total_amount
FROM retail_sales
WHERE gender = 'Female' AND age > 50
ORDER BY total_amount DESC
LIMIT 10
"""
df_female_over50 = pd.read_sql_query(query1, conn)
print("\n1. Топ-10 покупок женщин старше 50 лет:")
print(df_female_over50)

# Запрос 2: Покупки в категории Electronics с суммой больше 1000
query2 = """
SELECT date, customer_id, product_category, quantity, total_amount
FROM retail_sales
WHERE product_category = 'Electronics' AND total_amount > 1000
ORDER BY total_amount DESC
"""
df_electronics_high = pd.read_sql_query(query2, conn)
print("\n2. Покупки Electronics на сумму > 1000:")
print(df_electronics_high.head(10))

# Запрос 3: Молодые покупатели (18-25 лет) в категории Beauty
query3 = """
SELECT customer_id, age, gender, quantity, total_amount
FROM retail_sales
WHERE product_category = 'Beauty' AND age BETWEEN 18 AND 25
ORDER BY age
"""
df_beauty_young = pd.read_sql_query(query3, conn)
print("\n3. Молодые покупатели (18-25 лет) в категории Beauty:")
print(df_beauty_young.head(10))

# 2. SQL запросы с агрегатными функциями
print("\n" + "="*50)
print("АГРЕГАЦИОННЫЕ ЗАПРОСЫ")
print("="*50)

# Запрос 4: Статистика по возрасту и полу
query4 = """
SELECT 
    gender,
    COUNT(*) as transaction_count,
    AVG(age) as avg_age,
    MIN(age) as min_age,
    MAX(age) as max_age,
    SUM(total_amount) as total_sales,
    AVG(total_amount) as avg_transaction
FROM retail_sales
GROUP BY gender
"""
df_age_gender_stats = pd.read_sql_query(query4, conn)
print("\n4. Статистика по полу:")
print(df_age_gender_stats)

# Запрос 5: Продажи по полу в категориях
query5 = """
SELECT 
    product_category,
    gender,
    COUNT(*) as transactions,
    SUM(quantity) as total_items,
    SUM(total_amount) as total_sales,
    AVG(total_amount) as avg_transaction
FROM retail_sales
GROUP BY product_category, gender
ORDER BY product_category, gender
"""
df_category_gender = pd.read_sql_query(query5, conn)
print("\n5. Продажи по полу в категориях:")
print(df_category_gender)

# Запрос 6: Динамика продаж по месяцам
query6 = """
SELECT 
    strftime('%Y-%m', date) as month,
    gender,
    COUNT(*) as transactions,
    SUM(total_amount) as total_sales,
    AVG(total_amount) as avg_sale
FROM retail_sales
GROUP BY month, gender
ORDER BY month
"""
df_monthly = pd.read_sql_query(query6, conn)
print("\n6. Динамика продаж по месяцам:")
print(df_monthly.head(15))

# Запрос 7: Сравнение по возрастным группам
query7 = """
SELECT 
    CASE 
        WHEN age < 25 THEN '18-24'
        WHEN age BETWEEN 25 AND 34 THEN '25-34'
        WHEN age BETWEEN 35 AND 44 THEN '35-44'
        WHEN age BETWEEN 45 AND 54 THEN '45-54'
        ELSE '55+'
    END as age_group,
    COUNT(*) as transaction_count,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(total_amount) as total_sales,
    AVG(total_amount) as avg_transaction,
    SUM(quantity) as total_items,
    AVG(quantity) as avg_items
FROM retail_sales
GROUP BY age_group
ORDER BY 
    CASE age_group
        WHEN '18-24' THEN 1
        WHEN '25-34' THEN 2
        WHEN '35-44' THEN 3
        WHEN '45-54' THEN 4
        ELSE 5
    END
"""
df_age_groups = pd.read_sql_query(query7, conn)
print("\n7. Сравнение по возрастным группам:")
print(df_age_groups)

# 3. ВИЗУАЛИЗАЦИЯ ДАННЫХ
print("\n" + "="*50)
print("СОЗДАНИЕ ВИЗУАЛИЗАЦИЙ")
print("="*50)

# Настройка стиля для всех графиков
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

# 1. Продажи по возрасту и полу
fig, axes = plt.subplots(2, 3, figsize=(18, 12))
fig.suptitle('Анализ розничных продаж', fontsize=16, fontweight='bold')

# График 1: Распределение продаж по возрасту
df_all = pd.read_sql_query("SELECT age, gender, total_amount FROM retail_sales", conn)
for gender in df_all['gender'].unique():
    data = df_all[df_all['gender'] == gender]
    axes[0, 0].hist(data['age'], bins=20, alpha=0.6, label=gender, edgecolor='black')
axes[0, 0].set_xlabel('Возраст')
axes[0, 0].set_ylabel('Количество транзакций')
axes[0, 0].set_title('Распределение продаж по возрасту')
axes[0, 0].legend()
axes[0, 0].grid(True, alpha=0.3)

# График 2: Продажи по полу в категориях
df_cat = pd.read_sql_query("""
    SELECT product_category, gender, SUM(total_amount) as total_sales
    FROM retail_sales
    GROUP BY product_category, gender
""", conn)
pivot_cat = df_cat.pivot(index='product_category', columns='gender', values='total_sales')
pivot_cat.plot(kind='bar', ax=axes[0, 1], rot=0)
axes[0, 1].set_xlabel('Категория товара')
axes[0, 1].set_ylabel('Общая сумма продаж')
axes[0, 1].set_title('Продажи по полу в категориях')
axes[0, 1].legend(title='Пол')
axes[0, 1].grid(True, alpha=0.3)

# График 3: Динамика продаж по месяцам
df_monthly_pivot = df_monthly.pivot(index='month', columns='gender', values='total_sales')
df_monthly_pivot.plot(kind='line', marker='o', ax=axes[0, 2])
axes[0, 2].set_xlabel('Месяц')
axes[0, 2].set_ylabel('Общая сумма продаж')
axes[0, 2].set_title('Динамика продаж по месяцам')
axes[0, 2].legend(title='Пол')
axes[0, 2].tick_params(axis='x', rotation=45)
axes[0, 2].grid(True, alpha=0.3)

# График 4: Сравнение по возрастным группам
bars = axes[1, 0].bar(df_age_groups['age_group'], df_age_groups['total_sales'],
                      color=['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7'])
axes[1, 0].set_xlabel('Возрастная группа')
axes[1, 0].set_ylabel('Общая сумма продаж')
axes[1, 0].set_title('Продажи по возрастным группам')
for bar in bars:
    height = bar.get_height()
    axes[1, 0].text(bar.get_x() + bar.get_width()/2., height,
                   f'{height:,.0f}', ha='center', va='bottom')
axes[1, 0].grid(True, alpha=0.3)

# График 5: Средний чек по возрастным группам
bars2 = axes[1, 1].bar(df_age_groups['age_group'], df_age_groups['avg_transaction'],
                       color=['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7'])
axes[1, 1].set_xlabel('Возрастная группа')
axes[1, 1].set_ylabel('Средний чек')
axes[1, 1].set_title('Средний чек по возрастным группам')
for bar in bars2:
    height = bar.get_height()
    axes[1, 1].text(bar.get_x() + bar.get_width()/2., height,
                   f'{height:.1f}', ha='center', va='bottom')
axes[1, 1].grid(True, alpha=0.3)

# График 6: Корреляционная матрица
df_corr = df_all[['age', 'total_amount']].copy()
df_corr['quantity'] = pd.read_sql_query("SELECT quantity FROM retail_sales", conn)['quantity']
corr_matrix = df_corr.corr()
sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', center=0,
            square=True, ax=axes[1, 2], linewidths=1, cbar_kws={"shrink": 0.8})
axes[1, 2].set_title('Корреляционная матрица')

plt.tight_layout()
plt.savefig('retail_sales_analysis.png', dpi=300, bbox_inches='tight')
plt.show()

# Дополнительная визуализация: Топ продуктов по количеству продаж
fig2, axes2 = plt.subplots(1, 2, figsize=(15, 6))

# Круговая диаграмма распределения по категориям
df_category = pd.read_sql_query("""
    SELECT product_category, SUM(total_amount) as total_sales, COUNT(*) as transactions
    FROM retail_sales
    GROUP BY product_category
""", conn)

colors = ['#FF9999', '#66B2FF', '#99FF99']
axes2[0].pie(df_category['total_sales'], labels=df_category['product_category'],
             autopct='%1.1f%%', colors=colors, startangle=90)
axes2[0].set_title('Распределение продаж по категориям', fontweight='bold')

# Количество транзакций по возрастным группам
bars3 = axes2[1].bar(df_age_groups['age_group'], df_age_groups['transaction_count'],
                     color=['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7'])
axes2[1].set_xlabel('Возрастная группа')
axes2[1].set_ylabel('Количество транзакций')
axes2[1].set_title('Количество транзакций по возрастным группам', fontweight='bold')
for bar in bars3:
    height = bar.get_height()
    axes2[1].text(bar.get_x() + bar.get_width()/2., height,
                 f'{int(height)}', ha='center', va='bottom')
axes2[1].grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('additional_analysis.png', dpi=300, bbox_inches='tight')
plt.show()

# Сохраняем все данные в Excel для дальнейшего анализа
with pd.ExcelWriter('retail_sales_analysis.xlsx', engine='openpyxl') as writer:
    df_female_over50.to_excel(writer, sheet_name='Женщины_50+', index=False)
    df_electronics_high.to_excel(writer, sheet_name='Electronics_1000+', index=False)
    df_beauty_young.to_excel(writer, sheet_name='Beauty_18-25', index=False)
    df_age_gender_stats.to_excel(writer, sheet_name='Статистика_по_полу', index=False)
    df_category_gender.to_excel(writer, sheet_name='Продажи_категории_пол', index=False)
    df_monthly.to_excel(writer, sheet_name='Динамика_помесячно', index=False)
    df_age_groups.to_excel(writer, sheet_name='Возрастные_группы', index=False)
    df_category.to_excel(writer, sheet_name='По_категориям', index=False)

print("\n" + "="*50)
print("ГОТОВО!")
print("="*50)
print("Созданы файлы:")
print("1. retail_sales_analysis.png - Основные графики")
print("2. additional_analysis.png - Дополнительные графики")
print("3. retail_sales_analysis.xlsx - Все данные в Excel")

# Закрываем соединение с базой данных
conn.close()