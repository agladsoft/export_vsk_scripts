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
    "Экспедитор": "expeditor",
    "Отправитель (исходное название)": "shipper_name",
    "Номер контейнера": "container_number",
    "Порт (предобработка)": "destination_port",
    "Страна (предобратока)": "destination_country",
    "Груз": "goods_name",
    "TEU": "teu",
    "Размер контейнера": "container_size",
    "Тип контейнера": "container_type",
    "Группа груза по ТНВЭД (проставляется вручную через код ТНВЭД - ячека Х)": "tnved_group_id",
    "Наименование Группы (подтягивается по коду через справочник)": "tnved_group_name",
    "ИНН (извлечен через excel)": "shipper_inn",
    "УНИ-компания (подтянута через ИНН)": "shipper_name_unified",
    "Страна": "shipper_country",
    "Регион компании": "shipper_region",
    "Номер ГТД": "gtd_number",
    "Тип Парка": "park_type",
    "ТНВЭД": "tnved",
    "Судно": "ship_name",
    "Получатель": "consignee_name",
    "Букинг": "booking"
}


df = pd.read_csv(input_file_path)
df = df.replace({np.nan: None})
df = df.rename(columns=headers_eng)
df = df.loc[:, ~df.columns.isin(['tnved_group_id', 'direction', 'tnved_group_name', 'shipper_inn',
                                 'shipper_name_unified', 'destination_country'])]
parsed_data = df.to_dict('records')
for dict_data in parsed_data:
    for key, value in dict_data.items():
        with contextlib.suppress(Exception):
            if key in ['year', 'month', 'teu', 'container_size', 'tnved_group_id']:
                dict_data[key] = int(value)
            elif key == 'terminal':
                dict_data[key] = os.environ.get('XL_VSK_EXPORT')

    dict_data['original_file_name'] = os.path.basename(input_file_path)
    dict_data['original_file_parsed_on'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
basename = os.path.basename(input_file_path)
output_file_path = os.path.join(output_folder, f'{basename}.json')
with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
    json.dump(parsed_data, f, ensure_ascii=False, indent=4)