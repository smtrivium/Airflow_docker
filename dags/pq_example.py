# excel_powerquery_etl.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

class ExcelPowerQuery:
    """
    Power Query-style трансформер для Excel файлов с пошаговой визуализацией
    """
    
    def __init__(self, excel_path: str, sheet_name: str = 0):
        self.excel_path = excel_path
        self.sheet_name = sheet_name
        self.original_df = None
        self.current_df = None
        self.steps = []
        self.step_counter = 0
        
        # Загружаем данные
        self._load_excel()
    
    def _load_excel(self):
        """Загрузка данных из Excel"""
        print("📥 ЗАГРУЗКА ДАННЫХ ИЗ EXCEL")
        print(f"Файл: {self.excel_path}")
        
        try:
            # Читаем Excel файл
            self.original_df = pd.read_excel(self.excel_path, sheet_name=self.sheet_name, header=None)
            self.current_df = self.original_df.copy()
            
            print(f"✅ Успешно загружено")
            print(f"📊 Размер данных: {self.original_df.shape}")
            print(f"📋 Лист: {self.sheet_name}")
            
            # Анализируем структуру Excel
            self._analyze_excel_structure()
            
            # Добавляем шаг загрузки
            self._add_step("Source", self.original_df, f"Загрузка из Excel: {self.excel_path}")
            
        except Exception as e:
            print(f"❌ Ошибка загрузки: {e}")
            raise
    
    def _analyze_excel_structure(self):
        """Анализ структуры Excel файла для поиска заголовков"""
        print("\n🔍 АНАЛИЗ СТРУКТУРЫ EXCEL")
        
        # Ищем строку с заголовками
        header_row = None
        for i in range(min(10, len(self.original_df))):
            row_values = [str(x) for x in self.original_df.iloc[i] if pd.notna(x)]
            non_empty_count = len(row_values)
            print(f"Строка {i}: {non_empty_count} непустых значений → {row_values[:5]}...")
            
            if non_empty_count >= 3:  # Если есть несколько значимых значений
                header_row = i
                print(f"✅ Найден заголовок в строке {i}")
                break
        
        self.header_row = header_row if header_row is not None else 0
        
        # Перезагружаем данные с правильным заголовком
        self.original_df = pd.read_excel(
            self.excel_path, 
            sheet_name=self.sheet_name, 
            header=self.header_row
        )
        self.current_df = self.original_df.copy()
    
    def _add_step(self, step_type: str, result_df: pd.DataFrame, description: str, details: dict = None):
        """Добавляет шаг в историю преобразований"""
        self.step_counter += 1
        
        step_info = {
            'step_number': self.step_counter,
            'step_type': step_type,
            'description': description,
            'shape_before': self.current_df.shape if hasattr(self, 'current_df') else (0, 0),
            'shape_after': result_df.shape,
            'columns_before': list(self.current_df.columns) if hasattr(self, 'current_df') else [],
            'columns_after': list(result_df.columns),
            'preview_data': result_df.head(5).copy(),
            'details': details or {},
            'timestamp': datetime.now().strftime("%H:%M:%S")
        }
        
        self.steps.append(step_info)
        self.current_df = result_df.copy()
    
    def show_pipeline(self):
        """Показывает весь пайплайн преобразований"""
        print("\n" + "=" * 100)
        print("🏭 ПАЙПЛАЙН ПРЕОБРАЗОВАНИЙ POWER QUERY")
        print("=" * 100)
        
        for step in self.steps:
            print(f"\n🎯 ШАГ {step['step_number']}: {step['description']}")
            print(f"   ⏰ {step['timestamp']} | 📐 {step['shape_before']} → {step['shape_after']}")
            
            # Детали изменений
            if step['shape_before'] != step['shape_after']:
                rows_diff = step['shape_after'][0] - step['shape_before'][0]
                cols_diff = step['shape_after'][1] - step['shape_before'][1]
                
                if rows_diff != 0:
                    print(f"   📊 Строки: {step['shape_before'][0]} → {step['shape_after'][0]} ({rows_diff:+d})")
                if cols_diff != 0:
                    print(f"   📊 Колонки: {step['shape_before'][1]} → {step['shape_after'][1]} ({cols_diff:+d})")
            
            # Изменения в колонках
            if step['columns_before'] != step['columns_after']:
                added = set(step['columns_after']) - set(step['columns_before'])
                removed = set(step['columns_before']) - set(step['columns_after'])
                
                if added:
                    print(f"   ➕ Добавлены: {list(added)}")
                if removed:
                    print(f"   ➖ Удалены: {list(removed)}")
            
            # Дополнительные детали
            if step['details']:
                print(f"   📋 Детали: {step['details']}")
            
            print(f"   👀 PREVIEW данных:")
            print(step['preview_data'].to_string(index=False))
            print("-" * 80)
    
    # МЕТОДЫ ПРЕОБРАЗОВАНИЙ
    
    def remove_columns(self, columns: list):
        """Удаляет указанные колонки"""
        df_result = self.current_df.drop(columns=columns, errors='ignore')
        self._add_step(
            "Remove Columns", 
            df_result, 
            f"Удаление колонок",
            {'removed_columns': columns, 'count': len(columns)}
        )
        return self
    
    def keep_columns(self, columns: list):
        """Оставляет только указанные колонки"""
        df_result = self.current_df[columns]
        removed_columns = set(self.current_df.columns) - set(columns)
        self._add_step(
            "Keep Columns", 
            df_result, 
            f"Фильтрация колонок",
            {'kept_columns': columns, 'removed_columns': list(removed_columns)}
        )
        return self
    
    def rename_columns(self, column_mapping: dict):
        """Переименовывает колонки"""
        df_result = self.current_df.rename(columns=column_mapping)
        self._add_step(
            "Rename Columns", 
            df_result, 
            f"Переименование колонок",
            {'mapping': column_mapping}
        )
        return self
    
    def clean_column_names(self):
        """Очищает названия колонок (только Unnamed, остальные оставляем как есть)"""
        new_columns = {}
        for col in self.current_df.columns:
            if 'Unnamed' in str(col):
                # Для Unnamed колонок создаем простые имена
                new_columns[col] = f'column_{list(self.current_df.columns).index(col)}'
            else:
                # Оригинальные названия оставляем как есть
                new_columns[col] = str(col).strip()
        
        df_result = self.current_df.rename(columns=new_columns)
        self._add_step(
            "Clean Names", 
            df_result, 
            f"Очистка названий колонок",
            {'new_names': new_columns}
        )
        return self
    
    def filter_rows(self, condition: str):
        """Фильтрует строки по условию"""
        initial_count = len(self.current_df)
        df_result = self.current_df.query(condition)
        removed_count = initial_count - len(df_result)
        
        self._add_step(
            "Filter Rows", 
            df_result, 
            f"Фильтрация строк",
            {'condition': condition, 'removed_rows': removed_count, 'kept_rows': len(df_result)}
        )
        return self
    
    def filter_rows_advanced(self, mask):
        """Фильтрует строки с использованием маски"""
        initial_count = len(self.current_df)
        df_result = self.current_df[mask]
        removed_count = initial_count - len(df_result)
        
        self._add_step(
            "Filter Rows Advanced", 
            df_result, 
            f"Фильтрация строк (advanced)",
            {'removed_rows': removed_count, 'kept_rows': len(df_result)}
        )
        return self
    
    def remove_empty_rows(self):
        """Удаляет полностью пустые строки"""
        initial_count = len(self.current_df)
        df_result = self.current_df.dropna(how='all')
        removed_count = initial_count - len(df_result)
        
        self._add_step(
            "Remove Empty Rows", 
            df_result, 
            f"Удаление пустых строк",
            {'removed_empty_rows': removed_count}
        )
        return self
    
    def remove_empty_columns(self):
        """Удаляет полностью пустые колонки"""
        initial_columns = len(self.current_df.columns)
        df_result = self.current_df.dropna(axis=1, how='all')
        removed_count = initial_columns - len(df_result.columns)
        
        self._add_step(
            "Remove Empty Columns", 
            df_result, 
            f"Удаление пустых колонок",
            {'removed_empty_columns': removed_count}
        )
        return self
    
    def add_calculated_column(self, column_name: str, expression: callable):
        """Добавляет вычисляемую колонку"""
        df_result = self.current_df.assign(**{column_name: expression})
        self._add_step(
            "Add Column", 
            df_result, 
            f"Добавление колонки: {column_name}",
            {'new_column': column_name, 'expression': expression.__name__ if hasattr(expression, '__name__') else 'lambda'}
        )
        return self
    
    def change_data_type(self, column: str, new_type: str):
        """Изменяет тип данных колонки"""
        df_result = self.current_df.copy()
        
        type_mapping = {
            'int': 'int32', 'float': 'float64', 'str': 'str',
            'datetime': 'datetime64[ns]'
        }
        
        if new_type in type_mapping:
            df_result[column] = df_result[column].astype(type_mapping[new_type])
        
        self._add_step(
            "Change Type", 
            df_result, 
            f"Изменение типа данных",
            {'column': column, 'old_type': str(self.current_df[column].dtype), 'new_type': new_type}
        )
        return self
    
    def group_aggregate(self, group_by: list, aggregations: dict):
        """Группировка и агрегация данных"""
        df_result = self.current_df.groupby(group_by).agg(aggregations).reset_index()
        
        # Упрощаем имена колонок после агрегации
        if isinstance(df_result.columns, pd.MultiIndex):
            df_result.columns = [f"{col[0]}_{col[1]}" if col[1] else col[0] for col in df_result.columns]
        
        self._add_step(
            "Group & Aggregate", 
            df_result, 
            f"Группировка и агрегация",
            {'group_by': group_by, 'aggregations': aggregations}
        )
        return self
    
    def sort_data(self, by: list, ascending: bool = True):
        """Сортировка данных"""
        df_result = self.current_df.sort_values(by=by, ascending=ascending)
        self._add_step(
            "Sort", 
            df_result, 
            f"Сортировка данных",
            {'sort_by': by, 'ascending': ascending}
        )
        return self
    
    def get_result(self):
        """Возвращает финальный результат"""
        return self.current_df
    
    def save_result(self, output_path: str):
        """Сохраняет результат в CSV"""
        self.current_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"💾 Результат сохранен: {output_path}")


def excel_powerquery_etl():
    """
    ETL процесс с Excel в стиле Power Query
    """
    # Конфигурация
    EXCEL_PATH = '/opt/airflow/excel_data/test.xlsx'
    OUTPUT_PATH = '/opt/airflow/data/powerquery_output.csv'
    
    print("🚀 ЗАПУСК POWER QUERY ETL ДЛЯ EXCEL")
    print("=" * 80)
    
    try:
        # Инициализация трансформера
        pq = ExcelPowerQuery(EXCEL_PATH)
        
        # ПАЙПЛАЙН ПРЕОБРАЗОВАНИЙ
        result = (pq
            # Шаг 1: Базовая очистка
            .remove_empty_rows()
            .remove_empty_columns()
            .clean_column_names()
            
            # Шаг 2: Переименование в английские названия
            .rename_columns({
                '№': 'id',
                'статья': 'article', 
                'План': 'plan',
                'Факт': 'fact',
                'Отклонение': 'deviation',
                'column_5': 'deviation_percent'
            })
            
            # Шаг 3: Фильтрация (используем английские названия)
            .filter_rows('plan.notna() and fact.notna()')
            
            # Шаг 4: Преобразование типов
            .change_data_type('id', 'int')
            .change_data_type('plan', 'float')
            .change_data_type('fact', 'float')
            .change_data_type('deviation', 'float')
            
            # Шаг 5: Добавление вычисляемых колонок
            .add_calculated_column('plan_fact_ratio', lambda x: (x['fact'] / x['plan'] * 100).round(2))
            .add_calculated_column('achievement_status', lambda x: np.where(
                x['fact'] >= x['plan'], 'achieved', 'not_achieved'
            ))
            .add_calculated_column('absolute_deviation', lambda x: abs(x['deviation']))
            
            # Шаг 6: Фильтрация по бизнес-правилам
            .filter_rows('plan > 0 and fact > 0')
            .filter_rows('absolute_deviation > 0')  # Только с отклонениями
            
            # Шаг 7: Агрегация для анализа
            .group_aggregate(
                group_by=['achievement_status'],
                aggregations={
                    'plan': ['sum', 'mean'],
                    'fact': ['sum', 'mean'],
                    'absolute_deviation': ['mean', 'max'],
                    'id': 'count'
                }
            )
            
            # Шаг 8: Финальные преобразования
            .rename_columns({
                'id_count': 'articles_count',
                'plan_sum': 'total_plan',
                'fact_sum': 'total_fact',
                'plan_mean': 'avg_plan',
                'fact_mean': 'avg_fact',
                'absolute_deviation_mean': 'avg_deviation',
                'absolute_deviation_max': 'max_deviation'
            })
            .sort_data(['articles_count'], ascending=False)
        )
        
        # ПОКАЗЫВАЕМ ВЕСЬ ПАЙПЛАЙН
        pq.show_pipeline()
        
        # СОХРАНЯЕМ РЕЗУЛЬТАТ
        pq.save_result(OUTPUT_PATH)
        
        # ФИНАЛЬНЫЙ ОТЧЕТ
        final_df = pq.get_result()
        print("\n" + "=" * 80)
        print("✅ ETL ПРОЦЕСС УСПЕШНО ЗАВЕРШЕН")
        print("=" * 80)
        print(f"📁 Исходный файл: {EXCEL_PATH}")
        print(f"📁 Результат: {OUTPUT_PATH}")
        print(f"📊 Итоговый размер: {final_df.shape}")
        print(f"🔢 Количество шагов: {len(pq.steps)}")
        print(f"🎯 Финальные данные:")
        print(final_df.to_string(index=False))
        
        return final_df
        
    except Exception as e:
        print(f"❌ ОШИБКА В ETL ПРОЦЕССЕ: {e}")
        # Показываем шаги до ошибки
        if 'pq' in locals():
            pq.show_pipeline()
        raise


def debug_excel_transformation(step_number: int = None):
    """
    Функция для отладки преобразований
    """
    EXCEL_PATH = '/opt/airflow/excel_data/test.xlsx'
    
    pq = ExcelPowerQuery(EXCEL_PATH)
    
    if step_number:
        # Показываем конкретный шаг
        if step_number <= len(pq.steps):
            step = pq.steps[step_number - 1]
            print(f"🔍 ДЕБАГ ШАГА {step_number}: {step['description']}")
            print(f"Данные на этом шаге:")
            print(step['preview_data'])
        else:
            print("❌ Шаг не найден")
    else:
        # Показываем все шаги
        pq.show_pipeline()


# Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'excel_powerquery_etl',
    default_args=default_args,
    description='Power Query-style ETL для Excel файлов с пошаговой визуализацией',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['excel', 'powerquery', 'etl', 'pandas'],
) as dag:

    etl_task = PythonOperator(
        task_id='excel_powerquery_processing',
        python_callable=excel_powerquery_etl,
    )

    # Дополнительная задача для отладки
    debug_task = PythonOperator(
        task_id='debug_transformations',
        python_callable=debug_excel_transformation,
        op_kwargs={'step_number': None},  # Можно указать конкретный шаг
    )

    etl_task >> debug_task