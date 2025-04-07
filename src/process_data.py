import pandas as pd
from pathlib import Path
import re
import warnings
import time

# --- Конфигурация ---
# Определяем базовую папку проекта
BASE_DIR = Path(__file__).resolve().parent.parent # Если скрипт в src/
# BASE_DIR = Path(__file__).resolve().parent # Если скрипт в корне проекта

SOURCE_FOLDER = BASE_DIR / "data" / "raw"
PROCESSED_DATA_FILE = BASE_DIR / "data" / "processed" / "processed_call_data.parquet" # !!! Папка с исходными Excel файлами

# Параметры парсинга (из вашего предыдущего скрипта)
header_rows = [2, 3, 4]
skip_rows_before_header = 2
header_level_to_use = 2
source_hierarchy_col_original_name = 'Имя 0'
source_hierarchy_col_new_name = 'Источник_Группа_Время'

# --- Функция обработки ОДНОГО Excel файла (Ваша логика из предыдущих шагов) ---
def parse_excel_file(file_path):
    print(f"Обработка файла: {file_path.name}...")
    try:
        # --- Шаг 1: Чтение и Упрощение Заголовков ---
        df = pd.read_excel(
            file_path,
            header=header_rows,
            skiprows=skip_rows_before_header,
            sheet_name=0 # Читаем первый лист
        )
        if isinstance(df.columns, pd.MultiIndex):
             if len(df.columns.levels) > header_level_to_use:
                df.columns = df.columns.get_level_values(header_level_to_use)
             else: # Fallback
                 df.columns = df.columns.get_level_values(0)

        if source_hierarchy_col_original_name in df.columns:
            df = df.rename(columns={source_hierarchy_col_original_name: source_hierarchy_col_new_name})
        elif not df.empty and not df.columns.empty:
             original_first_col = df.columns[0]
             if original_first_col != source_hierarchy_col_new_name:
                 df = df.rename(columns={original_first_col: source_hierarchy_col_new_name})

        # --- Шаг 2: Обработка Иерархии ---
        if source_hierarchy_col_new_name not in df.columns:
             print(f"  Предупреждение: Колонка '{source_hierarchy_col_new_name}' не найдена в {file_path.name}. Пропуск обработки иерархии.")
             return None # Или вернуть пустой df? pd.DataFrame()

        df['Имя_Сотрудника'] = pd.Series(dtype='object')
        df['Месяц_Год'] = pd.Series(dtype='object')
        df['Время'] = pd.Series(dtype='object')
        time_pattern = re.compile(r'^\d{2}:\d{2}$')
        month_year_pattern = re.compile(r'^[А-Яа-я]{3}\s\d{2}$')
        current_name = None
        current_month_year = None
        for index, row in df.iterrows():
            # ... (Ваша логика цикла обработки иерархии из предыдущего скрипта) ...
             value_obj = row.get(source_hierarchy_col_new_name)
             if pd.isna(value_obj): continue
             value = str(value_obj).strip()
             if month_year_pattern.match(value):
                 current_month_year = value
                 df.loc[index, 'Месяц_Год'] = current_month_year
             elif time_pattern.match(value):
                 df.loc[index, 'Время'] = value
                 df.loc[index, 'Имя_Сотрудника'] = current_name
                 df.loc[index, 'Месяц_Год'] = current_month_year
             else:
                 current_name = value
                 current_month_year = None
                 df.loc[index, 'Имя_Сотрудника'] = current_name

        df_flat = df[df['Время'].notna()].copy()
        if df_flat.empty:
            print(f"  Предупреждение: Нет данных после фильтрации по времени в {file_path.name}")
            return None

        # --- Шаг 3: Очистка и Приведение Типов ---
        potential_numeric_cols = [c for c in df_flat.columns if c not in [source_hierarchy_col_new_name, 'Имя_Сотрудника', 'Месяц_Год', 'Время']]
        numeric_cols = []
        for col in potential_numeric_cols:
            try:
                original_dtype = df_flat[col].dtype
                converted_col = pd.to_numeric(df_flat[col], errors='coerce')
                if not converted_col.isna().all():
                    df_flat[col] = converted_col
                    if col == 'Отправитель':
                        # Преобразуем в строку СРАЗУ
                        df_flat[col] = df_flat[col].astype(str).str.replace(r'\.0$', '', regex=True).replace('nan', pd.NA)
                    else:
                        numeric_cols.append(col) # Добавляем только реальные метрики
            except Exception as e:
                 print(f"  Ошибка приведения типа колонки {col} в {file_path.name}: {e}")
                 df_flat[col] = df_flat[col].astype('object')

        if source_hierarchy_col_new_name in df_flat.columns:
            df_flat = df_flat.drop(columns=[source_hierarchy_col_new_name])

        # --- Шаг 4: Melt ---
        all_cols = df_flat.columns.tolist()
        value_vars = numeric_cols
        id_vars = [col for col in all_cols if col not in value_vars]
        if not value_vars:
            print(f"  Предупреждение: нет числовых колонок для melt в {file_path.name}")
            # Решите, что возвращать: df_flat или None? Зависит, нужны ли данные без melt
            return df_flat # Возвращаем как есть, если melt невозможен
        df_melted = pd.melt(df_flat, id_vars=id_vars, value_vars=value_vars, var_name='Тип_Контакта', value_name='Количество')

        # Добавляем имя файла как источник (полезно для отладки)
        df_melted['Источник_Файл'] = file_path.name

        print(f"  Успешно обработан файл: {file_path.name}, строк: {len(df_melted)}")
        return df_melted

    except Exception as e:
        print(f"  !!! Ошибка обработки файла {file_path.name}: {e}")
        return None # Возвращаем None при ошибке

# --- Основной цикл обработки папки ---
def main():
    print("Запуск обработки данных...")
    SOURCE_FOLDER.mkdir(parents=True, exist_ok=True) # Создаем папку, если ее нет

    all_data = []
    processed_files_count = 0
    error_files_count = 0

    # Получаем время последнего изменения файла с обработанными данными (если он есть)
    try:
        last_processed_time = PROCESSED_DATA_FILE.stat().st_mtime
        print(f"Последние обработанные данные сохранены: {time.ctime(last_processed_time)}")
    except FileNotFoundError:
        last_processed_time = 0 # Обрабатываем все файлы, если файла нет
        print("Файл с обработанными данными не найден, будут обработаны все Excel файлы.")

    # Ищем все Excel файлы в папке
    excel_files = list(SOURCE_FOLDER.glob('*.xlsx')) + list(SOURCE_FOLDER.glob('*.xls'))
    print(f"Найдено {len(excel_files)} Excel файлов в '{SOURCE_FOLDER}'.")

    for file_path in excel_files:
        try:
            # Проверяем, нужно ли обрабатывать файл (новее, чем последняя обработка)
            file_mod_time = file_path.stat().st_mtime
            if file_mod_time > last_processed_time:
                print(f"\nНайден новый/измененный файл: {file_path.name} (изменен: {time.ctime(file_mod_time)})")
                processed_df = parse_excel_file(file_path)
                if processed_df is not None and not processed_df.empty:
                    all_data.append(processed_df)
                    processed_files_count += 1
                elif processed_df is None:
                    error_files_count += 1
            # else: # Раскомментировать для отладки
            #     print(f"Пропуск файла (не изменен): {file_path.name}")

        except Exception as e:
            print(f"!!! Критическая ошибка при доступе к файлу {file_path.name}: {e}")
            error_files_count += 1

    if not all_data and last_processed_time == 0:
         print("Не найдено данных для обработки в новых файлах.")
         # Если файла с результатами нет и новых данных нет, то выходим
         if not PROCESSED_DATA_FILE.exists():
              print("Нет исходных данных для создания файла результатов.")
              return

    if all_data:
        print(f"\nОбъединение данных из {processed_files_count} файлов...")
        new_data_df = pd.concat(all_data, ignore_index=True)
        print(f"Получено {len(new_data_df)} новых строк.")

        # --- Опционально: Загрузка старых данных и объединение ---
        if PROCESSED_DATA_FILE.exists() and last_processed_time > 0:
            print(f"Загрузка предыдущих данных из {PROCESSED_DATA_FILE}...")
            try:
                if PROCESSED_DATA_FILE.suffix == '.parquet':
                    old_data_df = pd.read_parquet(PROCESSED_DATA_FILE)
                elif PROCESSED_DATA_FILE.suffix == '.csv':
                    old_data_df = pd.read_csv(PROCESSED_DATA_FILE)
                else: # Добавить другие форматы если нужно
                     old_data_df = pd.DataFrame() # Пустой DF если формат неизвестен

                print(f"Объединение новых ({len(new_data_df)}) и старых ({len(old_data_df)}) данных...")
                # Важно: Нужно предусмотреть удаление дубликатов или перезапись,
                # если файлы могли быть обработаны повторно с изменениями.
                # Простой вариант: просто добавляем новые.
                # Более сложный: Идентифицировать строки из обновленных файлов и заменить их.
                # Пока просто объединяем:
                final_df = pd.concat([old_data_df, new_data_df], ignore_index=True)
                # Опционально: Удаление полных дубликатов
                final_df = final_df.drop_duplicates()

            except Exception as e:
                print(f"Ошибка загрузки старых данных: {e}. Перезаписываем файл только новыми данными.")
                final_df = new_data_df
        else:
             # Если старых данных нет, используем только новые
             final_df = new_data_df

        # --- Сохранение результата ---
        print(f"Сохранение {len(final_df)} строк в {PROCESSED_DATA_FILE}...")
        try:
             if PROCESSED_DATA_FILE.suffix == '.parquet':
                 final_df.to_parquet(PROCESSED_DATA_FILE, index=False)
             elif PROCESSED_DATA_FILE.suffix == '.csv':
                 final_df.to_csv(PROCESSED_DATA_FILE, index=False, encoding='utf-8-sig')
             print("Данные успешно сохранены.")
        except Exception as e:
             print(f"!!! Ошибка сохранения данных: {e}")

    else:
         print("\nНет новых или измененных файлов для обработки.")

    print(f"Обработка завершена. Успешно обработано: {processed_files_count} файлов. Ошибок: {error_files_count} файлов.")

if __name__ == "__main__":
    # Убираем лишние предупреждения pandas
    warnings.simplefilter(action='ignore', category=FutureWarning)
    warnings.filterwarnings('ignore', message="^Workbook contains no default style") # Openpyxl style warning
    main()
