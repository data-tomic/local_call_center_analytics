# Аналитика Колл-Центра (Локальный Пайплайн)

Этот проект представляет собой локальный пайплайн для обработки и анализа данных колл-центра из Excel-файлов со сложной структурой. Он включает скрипты для автоматической обработки данных, локальный веб-дашборд на Streamlit и интеграцию с Power BI Desktop.

## Возможности

*   Автоматическая обработка Excel-файлов (`.xls`, `.xlsx`) из указанной папки.
*   Парсинг сложных многоуровневых заголовков и иерархической структуры данных.
*   Преобразование данных в "аккуратный" (tidy) формат, подходящий для анализа.
*   Сохранение обработанных данных в эффективном формате Parquet (или CSV).
*   Локальный интерактивный веб-дашборд на Streamlit для быстрой визуализации ключевых метрик.
*   Файл Power BI Desktop (`.pbix`), подключенный к обработанным данным для углубленного анализа.
*   Полностью локальная работа без использования облачных сервисов.

## Структура Проекта

<pre>
local_call_center_analytics/
├── .gitignore          # Файлы, игнорируемые Git
├── requirements.txt    # Зависимости Python
├── README.md           # Этот файл
│
├── data/
│   ├── raw/            # Папка для ИСХОДНЫХ Excel файлов
│   │   ├── .gitkeep    # Файл для сохранения пустой папки в Git
│   │   └── sample_call_center_data.xls # Пример файла для теста (опционально)
│   └── processed/      # Папка для обработанных данных (создается скриптом, игнорируется Git)
│       └── .gitkeep    # Файл для сохранения пустой папки локально
│
├── reports/
│   └── Call_Center_Analysis.pbix   # Файл Power BI
│
└── src/                # Папка с исходным кодом Python
    ├── process_data.py # Скрипт обработки данных
    └── dashboard_app.py# Скрипт Streamlit дашборда
</pre>

## Установка

1.  **Клонировать репозиторий:**
    ```bash
    git clone <URL вашего репозитория>
    cd local_call_center_analytics
    ```
2.  **Создать и активировать виртуальное окружение (Рекомендуется):**
    ```bash
    python -m venv venv
    # Windows
    venv\Scripts\activate
    # macOS / Linux
    source venv/bin/activate
    ```
3.  **Установить зависимости:**
    ```bash
    pip install -r requirements.txt
    ```
4.  **Поместить исходные данные:** Скопируйте ваши реальные Excel-файлы с данными колл-центра в папку `data/raw/`.
    *(Примечание: Реальные данные не должны коммититься в Git, если они большие или содержат чувствительную информацию. Файл `.gitignore` настроен так, чтобы игнорировать содержимое `data/processed/`)*.

## Использование

1.  **Обработка данных:**
    Запустите скрипт для обработки Excel-файлов из `data/raw/` и сохранения результата в `data/processed/`.
    ```bash
    python src/process_data.py
    ```
    При первом запуске будут обработаны все файлы. При последующих - только новые или измененные.

2.  **Запуск Веб-Дашборда:**
    ```bash
    streamlit run src/dashboard_app.py
    ```
    Дашборд откроется в вашем браузере по адресу типа `http://localhost:8501`.

3.  **Работа с Power BI:**
    *   Откройте файл `reports/Call_Center_Analysis.pbix` в Power BI Desktop.
    *   При необходимости обновите путь к источнику данных, чтобы он указывал на файл `data/processed/processed_call_data.parquet` (или `.csv`) в папке вашего локального проекта.
    *   Нажмите "Обновить" в Power BI, чтобы загрузить последние обработанные данные.

## Конфигурация

*   Пути к папкам данных и имена файлов настраиваются в начале скриптов `process_data.py` и `dashboard_app.py`.
*   Параметры парсинга Excel (номера строк заголовков, пропуск строк и т.д.) настраиваются в `process_data.py`.

## Важные Замечания

*   **Конфиденциальность данных:** Не добавляйте в Git реальные Excel-файлы, если они содержат персональные или конфиденциальные данные. Используйте папку `data/raw/` локально.
*   **Доступ к дашборду:** Дашборд Streamlit по умолчанию доступен только на вашем компьютере (`localhost`). Для доступа из локальной сети используйте IP-адрес компьютера и убедитесь, что брандмауэр настроен соответствующим образом.
