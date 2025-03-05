import os
import json
import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from src.scripts.flat_export_vsk import ExportVSK, headers_eng
from src.scripts.parsed import ParsedDf


@pytest.fixture
def sample_dataframe():
    return pd.DataFrame({
        'shipment_date': pd.to_datetime(['2024-01-01']),
        'Год': [2024],
        'Мес': [1],
        'teu': [10],
        'container_size': [40],
        'tnved_group_id': [1001],
        'tnved': [12345],
        'container_count': [1],
        'is_empty': [0],
        'destination_country': ['Russia'],
        'tnved_group_name': ['Electronics'],
        'terminal': ['Moscow Terminal'],
        'shipper_country': ['China'],
        'shipper_name': ['ABC Corp'],
        'goods_weight_brutto': [1000.5],
        'tracking_seaport': ['Shanghai'],
        'expeditor': ['XYZ Logistics'],
        'container_type': ['Dry'],
        'shipper_name_unified': ['ABC Corp Unified'],
        'goods_weight_with_package': [1050.0],
        'line': ['Maersk'],
        'gtd_number': ['123456789'],
        'booking': ['BKG123'],
        'original_file_parsed_on': ['2024-03-01'],
        'shipper_inn': ['7701234567'],
        'original_file_name': ['shipment.xlsx'],
        'goods_name': ['Smartphones'],
        'ship_name': ['Ever Given'],
        'container_number': ['MSKU1234567'],
        'voyage': ['V123'],
        'consignee_name': ['DEF Importers'],
        'direction': ['import']
    })


@pytest.fixture
def temp_excel_file(tmp_path, sample_dataframe):
    file_path = tmp_path / "test.xlsx"
    sample_dataframe.to_excel(file_path, index=False)
    return file_path


@pytest.fixture
def temp_output_folder(tmp_path):
    folder = tmp_path / "output"
    folder.mkdir()
    return folder


@pytest.fixture
def export_nw(temp_excel_file, temp_output_folder):
    return ExportVSK(str(temp_excel_file), str(temp_output_folder))


def test_read_file(export_nw):
    df = pd.read_excel(export_nw.input_file_path, dtype={"ИНН": str})
    assert not df.empty
    assert "Год" in df.columns
    assert "Мес" in df.columns


def test_transformation_df(export_nw, sample_dataframe):
    df = sample_dataframe.copy()
    df = df.rename(columns=headers_eng)
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    assert "year" in df.columns
    assert "month" in df.columns
    assert "ship_name" in df.columns
    assert df["ship_name"].iloc[0] == "Ever Given"


def test_add_new_columns(export_nw, sample_dataframe):
    df = sample_dataframe.copy()
    export_nw.add_new_columns(df)
    assert "original_file_name" in df.columns
    assert "original_file_parsed_on" in df.columns

    assert df["original_file_name"].iloc[0] == os.path.basename(export_nw.input_file_path)

    try:
        datetime.strptime(df["original_file_parsed_on"].iloc[0], "%Y-%m-%d %H:%M:%S")
    except ValueError:
        pytest.fail("Неверный формат даты в столбце original_file_parsed_on")




def test_change_type_and_values(sample_dataframe):
    df = sample_dataframe.copy()
    df = df.rename(columns=headers_eng)
    ExportVSK.change_type_and_values(df)
    assert df['shipment_date'].dtype == object  # Проверяем, что дата стала строкой
    assert df['year'].dtype == 'Int64'
    assert df['month'].dtype == 'Int64'
    assert df['teu'].dtype == 'Int64'
    assert df['container_size'].dtype == 'Int64'
    assert df['tnved_group_id'].dtype == 'Int64'
    assert df['tnved'].dtype == 'Int64'
    assert df['container_count'].dtype == 'Int64'
    assert df['is_empty'].dtype == bool



def test_write_to_json(export_nw, sample_dataframe, temp_output_folder):
    df = sample_dataframe.copy()

    df = df.rename(columns=headers_eng)
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    export_nw.add_new_columns(df)
    ExportVSK.change_type_and_values(df)
    df = df.replace({np.nan: None, "NaT": None})
    df["direction"] = df["direction"].replace({"импорт": "import", "экспорт": "export", "каботаж": "cabotage"})
    parsed_data = df.to_dict("records")
    export_nw.write_to_json(parsed_data)

    output_file = temp_output_folder / (os.path.basename(export_nw.input_file_path) + ".json")
    assert output_file.exists()
    with open(output_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["year"] == 2024
    assert data[0]["direction"] == "import"


def test_main(export_nw):
    export_nw.main()
    output_file = os.path.join(export_nw.output_folder, os.path.basename(export_nw.input_file_path) + ".json")
    assert os.path.exists(output_file)
    with open(output_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert isinstance(data, list)
    assert len(data) == 1

    expected_keys = set(list(headers_eng.values()) + ["original_file_name", "original_file_parsed_on"])
    print("Missing keys:", expected_keys - set(data[0].keys()))
    assert expected_keys.issubset(set(data[0].keys()))

    assert data[0]["direction"] == "import"
