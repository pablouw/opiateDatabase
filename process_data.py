from collect_data import get_calibration_data, get_batch_data, get_file_time
from database import execute_table_changes, execute_batch, check_matching_file_entry_length
import pandas as pd
import xml.etree.ElementTree as ET
import re



def make_calibration_df(cmpd_dict, calibration_data_list):
    columns = ["xml_file", "modified_filename", "timestamp", "compound", "curve_type", "origin",
               "weighting", "slope", "rsquared", "cc", "int_std_id"]
    df = pd.DataFrame(calibration_data_list, columns=columns)
    df['int_std'] = df['int_std_id'].map(cmpd_dict)
    return df


def extract_data(xml_file, mod_file, timestamp, root):
    cmpd_dict, calibration_data, refer_dict = get_calibration_data(root, xml_file, mod_file)
    df_calibration = make_calibration_df(cmpd_dict, calibration_data)

    instrument, total_samples, batch_df = get_batch_data(root, xml_file, mod_file, refer_dict)
    if batch_df is None:
        return None, False, False
    batch_data = [xml_file, mod_file, timestamp, instrument, total_samples]
    return df_calibration, batch_data, batch_df


def link_indexes_from_db(table_data, replace_df):
    indexes = list()
    for result in table_data:
        indexes.append(result[0])
    index_dict = dict()
    indexes.sort()
    for ind, item in enumerate(indexes):
        index_dict[ind] = item
    replace_list = list()
    replacement_data = replace_df.T.to_dict()
    for k, v in replacement_data.items():
        v['_id'] = index_dict[k]
        replace_list.append(v)
    return replace_list


def replace_file_data(engine, file_data, db_data_to_replace, db_tables):
    cal_table, batch_table, results_table = db_tables
    cal_table_file_data, batch_table_file_data, results_table_file_data = db_data_to_replace
    calibration, batch, results = file_data
    calibration = link_indexes_from_db(cal_table_file_data, calibration)
    results = link_indexes_from_db(results_table_file_data, results)
    execute_table_changes(engine, cal_table, calibration)
    execute_table_changes(engine, results_table, results)

    batch_index = batch_table_file_data[0][0]
    batch_replace = {'_id': batch_index,
                     'xml_file': batch[0],
                     'modified_filename': batch[1],
                     'timestamp': batch[2],
                     'instrument': batch[3],
                     'numb_samples': batch[4]}
    execute_batch(engine, batch_table, batch_replace)


def read_xml(file):
    file += '.xml'
    root = ET.parse(file).getroot()
    xml = root.find('XMLFILE')
    timestamp = get_file_time(xml)
    return timestamp, root


def collect_and_replace_data_db(engine, xml_file, mod_file, db_table_list):
    timestamp, root = read_xml(xml_file)
    df_calib, batch_data, batch_df = extract_data(xml_file, mod_file, timestamp, root)
    file_data_list = [df_calib, batch_data, batch_df]
    if df_calib is None:
        return False
    db_data = check_matching_file_entry_length(engine, db_table_list, xml_file, file_data_list)
    if not any(db_data):
        return False
    replace_file_data(engine, file_data_list, db_data, db_table_list)
    return True
