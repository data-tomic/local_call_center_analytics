import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

# --- Конфигурация ---
BASE_DIR = Path(__file__).resolve().parent.parent # Если скрипт в src/
DATA_FILE = BASE_DIR / "data" / "processed" / "processed_call_data.parquet"
# ... остальной код скрипта ...
# DATA_FILE = Path("./processed_call_data.csv")

# --- Функция загрузки данных с кэшированием ---
@st.cache_data # Кэшируем данные, чтобы не читать файл при каждом взаимодействии
def load_data():
    if not DATA_FILE.exists():
        st.error(f"Файл данных не найден: {DATA_FILE}")
        st.info("Запустите скрипт обработки данных (например, process_data.py) для его создания.")
        return pd.DataFrame() # Возвращаем пустой DataFrame

    try:
        if DATA_FILE.suffix == '.parquet':
            df = pd.read_parquet(DATA_FILE)
        elif DATA_FILE.suffix == '.csv':
            df = pd.read_csv(DATA_FILE)
        else:
            st.error(f"Неподдерживаемый формат файла: {DATA_FILE.suffix}")
            return pd.DataFrame()

        # --- Базовая предобработка (если нужна) ---
        # Например, преобразование дат (если Месяц_Год и Время можно спарсить)
        # df['ДатаВремя'] = ... # Попытка создать datetime колонку
        # Преобразуем Количество в int, если возможно (после melt оно float)
        df['Количество'] = df['Количество'].fillna(0).astype(int)
        return df
    except Exception as e:
        st.error(f"Ошибка загрузки данных из {DATA_FILE}: {e}")
        return pd.DataFrame()

# --- Загрузка данных ---
df = load_data()

# --- Интерфейс Streamlit ---
st.set_page_config(layout="wide", page_title="Аналитика Колл-Центра")
st.title("📊 Аналитика Колл-Центра (Локальная версия)")

if df.empty:
    st.warning("Нет данных для отображения.")
    st.stop() # Останавливаем выполнение скрипта дашборда

# --- Боковая панель с фильтрами ---
st.sidebar.header("Фильтры")

# Фильтр по сотруднику
all_employees = ['Все'] + sorted(df['Имя_Сотрудника'].astype(str).unique())
selected_employee = st.sidebar.selectbox("Сотрудник:", all_employees)

# Фильтр по типу контакта
all_types = ['Все'] + sorted(df['Тип_Контакта'].unique())
selected_type = st.sidebar.selectbox("Тип Контакта:", all_types)

# TODO: Добавить фильтр по дате, если есть колонка ДатаВремя
# selected_date_range = st.sidebar.date_input("Период:", [min_date, max_date])

# --- Применение фильтров ---
df_filtered = df.copy()
if selected_employee != 'Все':
    df_filtered = df_filtered[df_filtered['Имя_Сотрудника'] == selected_employee]
if selected_type != 'Все':
    df_filtered = df_filtered[df_filtered['Тип_Контакта'] == selected_type]
# TODO: Применить фильтр по дате

if df_filtered.empty:
    st.warning("Нет данных, соответствующих выбранным фильтрам.")
    st.stop()

# --- Отображение Аналитики ---
st.header("Сводная информация")

# Метрики KPI
total_calls = df_filtered['Количество'].sum()
# Считаем уникальные взаимодействия (если нужно, требует группировки до melt или по ID)
# unique_interactions = df_filtered[['Имя_Сотрудника', 'Месяц_Год', 'Время', 'Отправитель']].drop_duplicates().shape[0]

col1, col2, col3 = st.columns(3)
col1.metric("Всего событий (строк)", f"{total_calls:,}")
# col2.metric("Уникальных взаимодействий", f"{unique_interactions:,}") # Пример

# Графики
st.header("Визуализации")

# 1. Распределение по типу контакта
calls_by_type = df_filtered.groupby('Тип_Контакта')['Количество'].sum().reset_index()
fig_type = px.bar(calls_by_type, x='Тип_Контакта', y='Количество', title="Распределение по типу контакта", text_auto=True)
st.plotly_chart(fig_type, use_container_width=True)

# 2. Топ сотрудников по количеству событий
calls_by_employee = df_filtered.groupby('Имя_Сотрудника')['Количество'].sum().reset_index().sort_values(by='Количество', ascending=False)
fig_employee = px.bar(calls_by_employee.head(10), x='Имя_Сотрудника', y='Количество', title="Топ сотрудников по кол-ву событий", text_auto=True)
st.plotly_chart(fig_employee, use_container_width=True)

# TODO: Добавить другие графики:
# - Динамика по времени (если есть ДатаВремя)
# - Распределение по часам/дням недели

# Таблица с данными
st.header("Детализированные данные")
st.dataframe(df_filtered, use_container_width=True)

# Кнопка для принудительного обновления данных (перезагружает кэш)
if st.sidebar.button("Обновить данные"):
    st.cache_data.clear()
    st.rerun()
