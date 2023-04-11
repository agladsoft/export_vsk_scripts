import os
import sys
import json
import contextlib
import numpy as np
import pandas as pd
from pandas import DataFrame
from datetime import datetime

headers_eng: dict = {
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


class ExportVSK(object):
    def __init__(self, input_file_path: str, output_folder: str):
        self.input_file_path: str = input_file_path
        self.output_folder: str = output_folder

    @staticmethod
    def trim_all_columns(df) -> DataFrame:
        """
        Delete the empty lines at the beginning and at the end of the lines.
        """
        return df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    @staticmethod
    def change_type_and_values(df) -> None:
        """
        Change data types or changing values.
        """
        with contextlib.suppress(Exception):
            df['shipment_date'] = df['shipment_date'].dt.date.astype(str)

    def write_to_json(self, parsed_data) -> None:
        """
        Write data to json.
        """
        basename: str = os.path.basename(self.input_file_path)
        output_file_path: str = os.path.join(self.output_folder, f'{basename}.json')
        with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
            json.dump(parsed_data, f, ensure_ascii=False, indent=4)

    def main(self) -> None:
        """
        The main function where we read the Excel file and write the file to json.
        """
        df: DataFrame = pd.read_excel(self.input_file_path)
        df = df.replace({np.nan: None})
        df = df.dropna(axis=0, how='all')
        df = df.rename(columns=headers_eng)
        df['original_file_name'] = os.path.basename(self.input_file_path)
        df['original_file_parsed_on'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        df = df.loc[:, ~df.columns.isin(['direction', 'tnved_group_name', 'shipper_inn',
                                         'shipper_name_unified', 'destination_country'])]
        df = self.trim_all_columns(df)
        self.change_type_and_values(df)
        self.write_to_json(df.to_dict('records'))


export_vsk: ExportVSK = ExportVSK(sys.argv[1], sys.argv[2])
export_vsk.main()
