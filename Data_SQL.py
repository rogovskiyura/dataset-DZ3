import sqlite3
import pandas as pd


def execute_sql_queries(db_file_path='retail_sales.db'):
    """Выполнение SQL-запросов для анализа данных"""

    conn = sqlite3.connect(db_file_path)

    # 1. Базовые запросы с фильтрацией WHERE
    print("=" * 80)
    print("1. БАЗОВЫЕ ЗАПРОСЫ С ФИЛЬТРАЦИЕЙ")
    print("=" * 80)

    # 1.1 Продажи для женщин старше 30 лет
    query1 = """
    SELECT customer_id, age, product_category, total_amount
    FROM retail_sales
    WHERE gender = 'Female' AND age > 30
    ORDER BY total_amount DESC
    LIMIT 10;
    """
    df1 = pd.read_sql_query(query1, conn)
    print("\n1.1 Топ-10 продаж (женщины старше 30 лет):")
    print(df1)

    # 1.2 Продажи в категории Electronics с высокой стоимостью
    query2 = """
    SELECT date, customer_id, age, gender, quantity, total_amount
    FROM retail_sales
    WHERE product_category = 'Electronics' AND total_amount > 1000
    ORDER BY total_amount DESC;
    """
    df2 = pd.read_sql_query(query2, conn)
    print(f"\n1.2 Продажи Electronics > 1000 (всего {len(df2)} записей):")
    print(df2.head())

    # 1.3 Продажи за последний квартал 2023 года
    query3 = """
    SELECT date, product_category, gender, total_amount
    FROM retail_sales
    WHERE date BETWEEN '2023-10-01' AND '2023-12-31'
    ORDER BY date;
    """
    df3 = pd.read_sql_query(query3, conn)
    print(f"\n1.3 Продажи в 4-м квартале 2023 (всего {len(df3)} записей):")
    print(df3.head())

    # 2. АГРЕГАТНЫЕ ФУНКЦИИ
    print("\n" + "=" * 80)
    print("2. АГРЕГАТНЫЕ ФУНКЦИИ")
    print("=" * 80)

    # 2.1 Продажи по возрасту и полу
    query4 = """
    SELECT 
        CASE 
            WHEN age BETWEEN 18 AND 25 THEN '18-25'
            WHEN age BETWEEN 26 AND 35 THEN '26-35'
            WHEN age BETWEEN 36 AND 45 THEN '36-45'
            WHEN age BETWEEN 46 AND 55 THEN '46-55'
            ELSE '55+'
        END as age_group,
        gender,
        COUNT(*) as transaction_count,
        SUM(total_amount) as total_sales,
        AVG(total_amount) as avg_transaction_value,
        SUM(quantity) as total_items
    FROM retail_sales
    GROUP BY age_group, gender
    ORDER BY age_group, gender;
    """
    df4 = pd.read_sql_query(query4, conn)
    print("\n2.1 Продажи по возрастным группам и полу:")
    print(df4)

    # 2.2 Продажи по полу в разрезе категорий
    query5 = """
    SELECT 
        product_category,
        gender,
        COUNT(*) as transaction_count,
        SUM(total_amount) as total_sales,
        AVG(total_amount) as avg_sale,
        SUM(quantity) as total_quantity
    FROM retail_sales
    GROUP BY product_category, gender
    ORDER BY product_category, gender;
    """
    df5 = pd.read_sql_query(query5, conn)
    print("\n2.2 Продажи по полу в категориях:")
    print(df5)

    # 2.3 Динамика продаж по месяцам с разбивкой по полу
    query6 = """
    SELECT 
        strftime('%Y-%m', date) as month,
        gender,
        COUNT(*) as transactions,
        SUM(total_amount) as monthly_sales,
        AVG(total_amount) as avg_transaction,
        SUM(quantity) as items_sold
    FROM retail_sales
    GROUP BY month, gender
    ORDER BY month, gender;
    """
    df6 = pd.read_sql_query(query6, conn)
    print("\n2.3 Динамика продаж по месяцам (по полу):")
    print(df6)

    # 3. ДОПОЛНИТЕЛЬНЫЕ АНАЛИТИЧЕСКИЕ ЗАПРОСЫ

    # 3.1 Корреляционный анализ (подготовка данных для матрицы)
    query7 = """
    SELECT 
        age,
        CASE WHEN gender = 'Male' THEN 1 ELSE 0 END as is_male,
        quantity,
        price_per_unit,
        total_amount,
        CASE 
            WHEN product_category = 'Electronics' THEN 1
            WHEN product_category = 'Clothing' THEN 2
            WHEN product_category = 'Beauty' THEN 3
        END as category_code
    FROM retail_sales;
    """
    df7 = pd.read_sql_query(query7, conn)
    print("\n3.1 Данные для корреляционной матрицы (первые 10 строк):")
    print(df7.head(10))

    # Корреляционная матрица
    print("\nКорреляционная матрица числовых признаков:")
    print(df7.corr().round(3))

    # 3.2 Сравнение по возрастным группам (вместо топ-10 клиентов)
    query8 = """
    WITH age_group_stats AS (
        SELECT 
            CASE 
                WHEN age BETWEEN 18 AND 25 THEN '18-25 (Молодежь)'
                WHEN age BETWEEN 26 AND 35 THEN '26-35 (Молодые взрослые)'
                WHEN age BETWEEN 36 AND 45 THEN '36-45 (Средний возраст)'
                WHEN age BETWEEN 46 AND 55 THEN '46-55 (Зрелые)'
                ELSE '55+ (Пенсионный)'
            END as age_group,
            COUNT(DISTINCT customer_id) as unique_customers,
            COUNT(*) as total_transactions,
            SUM(total_amount) as total_revenue,
            AVG(total_amount) as avg_transaction_value,
            SUM(quantity) as total_items,
            AVG(quantity) as avg_items_per_transaction,
            SUM(total_amount) / COUNT(DISTINCT customer_id) as revenue_per_customer
        FROM retail_sales
        GROUP BY age_group
    )
    SELECT 
        age_group,
        unique_customers,
        total_transactions,
        ROUND(total_revenue, 2) as total_revenue,
        ROUND(avg_transaction_value, 2) as avg_transaction,
        total_items,
        ROUND(avg_items_per_transaction, 2) as avg_items,
        ROUND(revenue_per_customer, 2) as revenue_per_customer,
        ROUND(100.0 * total_transactions / SUM(total_transactions) OVER(), 2) as transactions_percentage
    FROM age_group_stats
    ORDER BY 
        CASE age_group
            WHEN '18-25 (Молодежь)' THEN 1
            WHEN '26-35 (Молодые взрослые)' THEN 2
            WHEN '36-45 (Средний возраст)' THEN 3
            WHEN '46-55 (Зрелые)' THEN 4
            ELSE 5
        END;
    """
    df8 = pd.read_sql_query(query8, conn)
    print("\n3.2 Сравнение показателей по возрастным группам:")
    print(df8)

    # 3.3 Дополнительная визуализация: Анализ популярности категорий по возрастным группам
    query9 = """
    SELECT 
        CASE 
            WHEN age BETWEEN 18 AND 25 THEN '18-25'
            WHEN age BETWEEN 26 AND 35 THEN '26-35'
            WHEN age BETWEEN 36 AND 45 THEN '36-45'
            WHEN age BETWEEN 46 AND 55 THEN '46-55'
            ELSE '55+'
        END as age_group,
        product_category,
        COUNT(*) as purchases,
        SUM(total_amount) as category_revenue,
        AVG(total_amount) as avg_spent,
        SUM(quantity) as items_bought,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY 
            CASE 
                WHEN age BETWEEN 18 AND 25 THEN '18-25'
                WHEN age BETWEEN 26 AND 35 THEN '26-35'
                WHEN age BETWEEN 36 AND 45 THEN '36-45'
                WHEN age BETWEEN 46 AND 55 THEN '46-55'
                ELSE '55+'
            END), 2) as category_percentage
    FROM retail_sales
    GROUP BY age_group, product_category
    ORDER BY age_group, category_revenue DESC;
    """
    df9 = pd.read_sql_query(query9, conn)
    print("\n3.3 Популярность категорий по возрастным группам:")
    print(df9)

    # 3.4 Сезонность продаж (по месяцам)
    query10 = """
    SELECT 
        strftime('%m', date) as month_num,
        CASE strftime('%m', date)
            WHEN '01' THEN 'Январь'
            WHEN '02' THEN 'Февраль'
            WHEN '03' THEN 'Март'
            WHEN '04' THEN 'Апрель'
            WHEN '05' THEN 'Май'
            WHEN '06' THEN 'Июнь'
            WHEN '07' THEN 'Июль'
            WHEN '08' THEN 'Август'
            WHEN '09' THEN 'Сентябрь'
            WHEN '10' THEN 'Октябрь'
            WHEN '11' THEN 'Ноябрь'
            WHEN '12' THEN 'Декабрь'
        END as month_name,
        COUNT(*) as transactions,
        SUM(total_amount) as total_sales,
        AVG(total_amount) as avg_sale,
        SUM(quantity) as total_items,
        COUNT(DISTINCT customer_id) as unique_customers
    FROM retail_sales
    GROUP BY month_num
    ORDER BY month_num;
    """
    df10 = pd.read_sql_query(query10, conn)
    print("\n3.4 Сезонность продаж по месяцам:")
    print(df10)

    conn.close()

    # Возвращаем DataFrames для возможной визуализации
    return {
        'age_gender_sales': df4,
        'category_gender_sales': df5,
        'monthly_sales': df6,
        'correlation_data': df7,
        'age_group_comparison': df8,
        'category_by_age': df9,
        'seasonality': df10
    }


# Выполнение запросов
if __name__ == "__main__":
    results = execute_sql_queries()

    print("\n" + "=" * 80)
    print("АНАЛИТИЧЕСКИЕ ВЫВОДЫ:")
    print("=" * 80)

    # Краткая аналитика по возрастным группам
    print("\nКлючевые показатели по возрастным группам:")
    age_data = results['age_group_comparison']
    top_age_group = age_data.loc[age_data['total_revenue'].idxmax()]
    print(f"- Наибольшая выручка: {top_age_group['age_group']} - {top_age_group['total_revenue']:,.2f}")
    print(
        f"- Средний чек варьируется от {age_data['avg_transaction'].min():.2f} до {age_data['avg_transaction'].max():.2f}")

    # Анализ по категориям
    print("\nПопулярность категорий:")
    cat_data = results['category_by_age']
    for category in ['Electronics', 'Clothing', 'Beauty']:
        cat_revenue = cat_data[cat_data['product_category'] == category]['category_revenue'].sum()
        print(f"- {category}: {cat_revenue:,.2f}")