import datetime
import json
import os
import sys
import contextlib
import numpy as np
import pandas as pd

input_file_path = os.path.abspath(sys.argv[1])
output_folder = sys.argv[2]

headers_eng = {
    "Год": "year",
    "Мес": "month",
    "Отгружен": "shipment_date",
    "Терминал": "terminal",
    "Направление": "direction",
    "Линия": "line",
    "Рейс": "voyage",
    "Экспедитор": "expeditor",
    "Отправитель (исходное название)": "shipper_name",
    "Номер контейнера": "container_number",
    "Порт (предобработка)": "tracking_seaport",
    "Страна (предобратока)": "destination_country",
    "Груз": "goods_name",
    "TEU": "teu",
    "Вес нетто": "goods_weight_netto",
    "Вес брутто": "goods_weight_brutto",
    "Размер контейнера": "container_size",
    "Тип контейнера": "container_type",
    "Кол-во контейнеров, шт.": "container_count",
    "Группа груза по ТНВЭД (проставляется вручную через код ТНВЭД - ячека Х)": "tnved_group_id",
    "Наименование Группы (подтягивается по коду через справочник)": "tnved_group_name",
    "ИНН (извлечен через excel)": "shipper_inn",
    "УНИ-компания (подтянута через ИНН)": "shipper_name_unified",
    "Страна": "shipper_country",
    "Номер ГТД": "gtd_number",
    "Порожний": "is_empty",
    "ТНВЭД": "tnved",
    "Судно": "ship_name",
    "Получатель": "consignee_name",
    "Букинг": "booking"
}


def trim_all_columns(df):
    """
    Trim whitespace from ends of each value across all series in dataframe
    """
    trim_strings = lambda x: x.strip() if isinstance(x, str) else x
    return df.applymap(trim_strings)


df = pd.read_csv(input_file_path)
df = df.replace({np.nan: None})
df = df.rename(columns=headers_eng)
df = df.loc[:, ~df.columns.isin(['direction', 'tnved_group_name', 'shipper_inn',
                                 'shipper_name_unified', 'destination_country'])]
df = trim_all_columns(df)
parsed_data = df.to_dict('records')
for dict_data in parsed_data:
    for key, value in dict_data.items():
        with contextlib.suppress(Exception):
            if key in ['year', 'month', 'teu', 'container_size']:
                dict_data[key] = int(value)
            elif key in ['tnved_group_id']:
                dict_data[key] = f"{int(value)}"
            elif key == 'terminal':
                dict_data[key] = os.environ.get('XL_VSK_EXPORT')
            elif key == 'shipment_date':
                dict_data[key] = str(datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S").date())
            elif key in ['is_empty']:
                dict_data[key] = value in ['1', 1, 'да', 'Да', 'True']
            elif key in ['goods_weight_netto', 'goods_weight_brutto']:
                dict_data[key] = float(value)
            elif key in ['container_count']:
                dict_data[key] = int(value)

    dict_data['original_file_name'] = os.path.basename(input_file_path)
    dict_data['original_file_parsed_on'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
basename = os.path.basename(input_file_path)
output_file_path = os.path.join(output_folder, f'{basename}.json')
with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
    json.dump(parsed_data, f, ensure_ascii=False, indent=4)