from datetime import datetime, timedelta
import pandas as pd
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def excel_to_csv_proper():
    """
    Функция для правильного чтения Excel файла и преобразования в CSV
    с детальным логированием каждого шага
    """
    try:
        # Пути к файлам
        excel_file_path = '/opt/airflow/excel_data/test.xlsx'
        csv_output_path = '/opt/airflow/data/output.csv'
        
        print("=" * 50)
        print("ШАГ 1: ПРОВЕРКА ДОСТУПНОСТИ ФАЙЛОВ")
        print("=" * 50)
        
        # Проверяем существование файла
        if not os.path.exists(excel_file_path):
            available_files = os.listdir('/opt/airflow/excel_data') if os.path.exists('/opt/airflow/excel_data') else []
            print(f"✗ Excel файл не найден: {excel_file_path}")
            print(f"Доступные файлы в папке: {available_files}")
            raise FileNotFoundError(f"Excel файл не найден: {excel_file_path}")
        
        print(f"✓ Excel файл найден: {excel_file_path}")
        file_size = os.path.getsize(excel_file_path)
        print(f"Размер файла: {file_size} байт")
        
        print("\n" + "=" * 50)
        print("ШАГ 2: ДИАГНОСТИКА СТРУКТУРЫ EXCEL")
        print("=" * 50)
        
        # Анализируем структуру Excel файла
        excel_file = pd.ExcelFile(excel_file_path)
        print(f"Количество листов: {len(excel_file.sheet_names)}")
        print(f"Имена листов: {excel_file.sheet_names}")
        
        # Читаем сырые данные для анализа структуры
        df_raw = pd.read_excel(excel_file_path, sheet_name=0, header=None)
        print(f"Размер сырых данных: {df_raw.shape} (строки x колонки)")
        
        print("\nПервые 5 строк сырых данных:")
        for i in range(min(5, len(df_raw))):
            print(f"Строка {i}: {df_raw.iloc[i].tolist()}")
        
        print("\n" + "=" * 50)
        print("ШАГ 3: ПОИСК ЗАГОЛОВКОВ ДАННЫХ")
        print("=" * 50)
        
        # Ищем строку с заголовками
        header_row = None
        for i in range(min(10, len(df_raw))):
            row_values = [str(x) for x in df_raw.iloc[i] if pd.notna(x)]
            non_empty_count = len(row_values)
            print(f"Строка {i}: {non_empty_count} непустых значений → {row_values}")
            
            if non_empty_count >= 3:  # Если в строке есть несколько значимых значений
                header_row = i
                print(f"✓ Найден заголовок в строке {i}")
                break
        
        if header_row is None:
            header_row = 0
            print("⚠ Заголовок не найден, используем первую строку")
        else:
            print(f"✓ Используем строку {header_row} как заголовок")
        
        print("\n" + "=" * 50)
        print("ШАГ 4: ЧТЕНИЕ ДАННЫХ С ПРАВИЛЬНЫМ ЗАГОЛОВКОМ")
        print("=" * 50)
        
        # Читаем данные с найденным заголовком
        df = pd.read_excel(
            excel_file_path,
            sheet_name=0,
            header=header_row
        )
        
        print(f"Размер данных после чтения: {df.shape}")
        print(f"Колонки: {df.columns.tolist()}")
        print(f"Типы колонок:")
        for col in df.columns:
            print(f"  {col}: {df[col].dtype}")
        
        print("\nПервые 3 строки данных:")
        print(df.head(3))
        
        print("\n" + "=" * 50)
        print("ШАГ 5: ОЧИСТКА ДАННЫХ - УДАЛЕНИЕ ПУСТЫХ КОЛОНОК")
        print("=" * 50)
        
        # Удаляем полностью пустые колонки
        initial_columns = len(df.columns)
        df_clean = df.dropna(axis=1, how='all')
        removed_columns = initial_columns - len(df_clean.columns)
        
        print(f"Было колонок: {initial_columns}")
        print(f"Удалено пустых колонок: {removed_columns}")
        print(f"Осталось колонок: {len(df_clean.columns)}")
        print(f"Колонки после удаления пустых: {df_clean.columns.tolist()}")
        
        print("\n" + "=" * 50)
        print("ШАГ 6: ОЧИСТКА ДАННЫХ - УДАЛЕНИЕ ПУСТЫХ СТРОК")
        print("=" * 50)
        
        # Удаляем полностью пустые строки
        initial_rows = len(df_clean)
        df_clean = df_clean.dropna(axis=0, how='all')
        removed_rows = initial_rows - len(df_clean)
        
        print(f"Было строк: {initial_rows}")
        print(f"Удалено пустых строк: {removed_rows}")
        print(f"Осталось строк: {len(df_clean)}")
        print("\n" + "=" * 50)
        print("ШАГ 7: СОХРАНЕНИЕ В CSV")
        print("=" * 50)
        
        # Создаем директорию если нужно
        os.makedirs(os.path.dirname(csv_output_path), exist_ok=True)
        
        # Сохраняем в CSV с правильной кодировкой
        df_clean.to_csv(csv_output_path, index=False, encoding='utf-8-sig')
        
        # Проверяем результат
        if os.path.exists(csv_output_path):
            file_size = os.path.getsize(csv_output_path)
            print(f"✓ CSV файл успешно создан: {csv_output_path}")
            print(f"Размер CSV файла: {file_size} байт")
            
            # Читаем CSV для проверки
            df_check = pd.read_csv(csv_output_path, encoding='utf-8-sig')
            print(f"Проверка: CSV содержит {len(df_check)} строк и {len(df_check.columns)} колонок")
            
            print("\nПервые 3 строки из CSV файла:")
            print(df_check.head(3))
        else:
            raise Exception("CSV файл не был создан")
        
        print("\n" + "=" * 50)
        print("ИТОГОВЫЙ ОТЧЕТ")
        print("=" * 50)
        print(f"✓ Обработка завершена успешно!")
        print(f"✓ Исходный файл: {excel_file_path}")
        print(f"✓ Результирующий файл: {csv_output_path}")
        print(f"✓ Обработано строк: {len(df_clean)}")
        print(f"✓ Обработано колонок: {len(df_clean.columns)}")
        print(f"✓ Удалено пустых строк: {removed_rows}")
        print(f"✓ Удалено пустых колонок: {removed_columns}")
      
        
    except Exception as e:
        print("\n" + "=" * 50)
        print("ОШИБКА ВЫПОЛНЕНИЯ")
        print("=" * 50)
        print(f"Тип ошибки: {type(e).__name__}")
        print(f"Сообщение: {str(e)}")
        print(f"Место ошибки: {excel_file_path}")
        raise

# Создаем DAG
with DAG(
    'excel_to_csv_detailed',
    default_args=default_args,
    description='DAG для конвертации Excel в CSV с детальным логированием шагов',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['excel', 'pandas', 'csv', 'detailed'],
) as dag:

    excel_conversion_task = PythonOperator(
        task_id='convert_excel_to_csv_detailed',
        python_callable=excel_to_csv_proper,
    )

    excel_conversion_task