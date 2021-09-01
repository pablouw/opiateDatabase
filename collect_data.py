from datetime import datetime as dt
from verify_data import replace_comp_spelling
import numpy as np
import pandas as pd
import re


def add_sample_label(df):
    sample_type = []
    regexs_controls = ['^neg', '^50', '^low', '^high', '^new', '^amr', '^new', '^opil']
    combined = "(" + ")|(".join(regexs_controls) + ")"
    for item in df['sample']:
        if re.search(combined, item, re.IGNORECASE):
            sample_type.append('control')
        elif re.search('^std', item, re.IGNORECASE):
            if re.search('x10', item, re.IGNORECASE):
                sample_type.append('control')
            else:
                sample_type.append('calibrator')
        elif re.search('^\D[0-9]{5,7}', item):
            sample_type.append('unknown')
        else:
            sample_type.append(None)

    df['sample_type'] = sample_type
    return df


def attach_sample_info(xml_file, mod_file, lc_batch, samplename, sampleid, injection_time, vial, df):
    df['xml_file'] = xml_file
    df['modified_filename'] = mod_file
    df['lc_batch'] = lc_batch
    df['sample'] = samplename
    df['sample_id'] = sampleid
    df['injection_time'] = injection_time
    df['vial'] = vial
    return df


def get_sample_results(sample):
    cmpd_data, is_data = [], []

    for compound in sample.findall('COMPOUND'):
        comp_name = compound.get('name')
        comp_name = replace_comp_spelling(comp_name)
        comp_id = compound.get('id')
        if re.search('[Dd][3-9]', comp_name) is None:  # Analyte Section #####
            for peak in compound.findall('PEAK'):
                pkarea = float(peak.get('area'))  # Peak Area of analyte
                analconc = peak.get('analconc')
                rt = float(peak.get('foundrt'))  # Actual RT of analyte
                signoise = peak.get('signoise')  # S/N of analyte
                if signoise == '':
                    signoise = np.NaN
                confirmationionpeak = peak.find('CONFIRMATIONIONPEAK1')
                if confirmationionpeak is not None:
                    cnf_area = float(confirmationionpeak.get('area'))  # Confirmation ion area for ion ratio
                else:
                    cnf_area = np.NaN
                cmpd_data.append([comp_name, comp_id, pkarea, analconc, rt, signoise, cnf_area])

        else:  # Internal Standard Section ####
            for peak in compound.findall('PEAK'):
                isrt = float(peak.get('foundrt'))  # Actual RT of I.S.
                ispeakarea = float(peak.get('area'))  # Peak area of I.S
                issignoise = peak.get('signoise')  # S/N of I.S.
                if issignoise == '':
                    issignoise = np.NaN
                confirmationionpk = peak.find('CONFIRMATIONIONPEAK1')
                if confirmationionpk is not None:
                    is_cnf_area = float(confirmationionpk.get('area'))  # Confirming ion peak area for ion ratio
                else:
                    is_cnf_area = np.NaN
                is_data.append([comp_name, comp_id, ispeakarea, isrt, issignoise, is_cnf_area])

    return cmpd_data, is_data


def make_cmpd_df(cmpd_data, columns_list, reference_dict=None):
    df = pd.DataFrame(cmpd_data, columns=columns_list)
    if columns_list[0] == 'compound':
        df['int_std_id'] = df['compound_id'].map(reference_dict)
    return df


def get_sample_data(samplelistdata, xml_file, mod_file, reference_dict):
    columns = ["compound", "compound_id", "peak_area", "concentration",
               "RT", "SN", "confirming_ion_area"]
    is_columns = ["int_std", "int_std_id", "is_peak_area", "is_RT", "is_SN", "is_confirming_ion_area"]
    batchdf = pd.DataFrame()

    for sample in samplelistdata:
        if sample.get('desc') == 'Water':
            continue
        lc_batch = sample.get('job')
        samplename = sample.get('desc')
        sampleid = sample.get('id')
        # date_obj = dt.strptime(sample.get('createdate'), '%d-%b-%y')
        date_obj = dt.strptime(sample.get('createdate'), '%d-%b-%Y')
        date = dt.strftime(date_obj, "%Y-%m-%d")
        time = sample.get('createtime')
        injection_time = date + ' ' + time
        vial = sample.get('vial')
        cmpd_data, is_data = get_sample_results(sample)

        df = make_cmpd_df(cmpd_data, columns, reference_dict)

        dfi = make_cmpd_df(is_data, is_columns)
        df_merged = df.merge(dfi, how='outer', on=['int_std_id'])
        df_merged = attach_sample_info(xml_file, mod_file, lc_batch, samplename,
                                      sampleid, injection_time, vial, df_merged)
        batchdf = batchdf.append(df_merged, ignore_index=True, sort=False)

    batchdf = add_sample_label(batchdf)
    batchdf[["concentration", "sample_id"]] = batchdf[["concentration", "sample_id"]].apply(pd.to_numeric)
    df_order = [13, 14, 17, 18, 15, 16, 19, 20] + list(range(0, 13))
    batchdf = batchdf.iloc[:, df_order]
    return batchdf


def get_batch_data(root, xml_file, mod_file, reference_dict):
    samplelistdata = root[2][0][1]
    total_samples = int(samplelistdata.get('count'))
    instrument = root[2][0][1][0].get('instrument')
    d, missing_sample_list = dict(), list()
    for sample in samplelistdata:
        instr = sample.get('instrument')
        if instr == "":
            missing_sample_list.append((sample.get('id'), sample.get('desc')))
        if instr in d:
            d[instr] += 1
        else:
            d[instr] = 1
    if total_samples != d[instrument]:
        print(f'Warning! {xml_file}')
        print(f'{total_samples-d[instrument]} of {total_samples} total samples have missing data.')
        print(f'See list of samples: {missing_sample_list}')
        print(f'Will not add to db. Revise file and try again.')
        print(' ')
        return instrument, total_samples, None
    batch_df = get_sample_data(samplelistdata, xml_file, mod_file, reference_dict)
    return instrument, total_samples, batch_df


def get_calibration_data(root, filename, mod_file):
    calibrationdata = root[2][0][2]
    date_obj = dt.strptime(calibrationdata.get('modifieddate'), '%d %b %Y')
    date = dt.strftime(date_obj, "%Y-%m-%d")
    time = calibrationdata.get('modifiedtime')
    timestamp = date + " " + time
    cmpd_dict = {}
    reference_dict = {}
    calibration_data = []
    for compound in calibrationdata.findall('COMPOUND'):
        cmpd_name = compound.get('name')
        cmpd_name = replace_comp_spelling(cmpd_name)
        cmpd_id = compound.get('id')
        cmpd_dict[cmpd_id] = cmpd_name
        for response in compound.findall('RESPONSE'):
            if response.get('type') != 'Internal Std':
                continue
            else:
                refer_cmpd = response.get('ref')
                reference_dict[cmpd_id] = refer_cmpd
                for curve in compound.findall('CURVE'):
                    curve_type = curve.get('type')
                    if curve_type == 'Linear':
                        origin = curve.get('origin')
                        weighting = curve.get('weighting')
                        cal_curve = curve[0]
                        cal_curve = cal_curve.get('curve')
                        for item in curve.findall('DETERMINATION'):
                            rsquared = item.get('rsquared')
                            calibration_data.append([filename, mod_file, timestamp, cmpd_name, curve_type, origin,
                                                     weighting, cal_curve, rsquared, np.NaN, refer_cmpd])
                    elif curve_type == 'RF':
                        for response in curve.findall('RESPONSEFACTOR'):
                            cc = response.get('cc')
                            calibration_data.append([filename, mod_file, timestamp, cmpd_name, curve_type, np.NaN, np.NaN,
                                                     np.NaN, np.NaN, cc, refer_cmpd])

    return cmpd_dict, calibration_data, reference_dict


def get_file_time(child):
    time = child.get('modifiedtime')
    date_obj = dt.strptime(child.get('modifieddate'), '%d %b %Y')
    date = dt.strftime(date_obj, "%Y-%m-%d")
    timestamp = date + ' ' + time
    return timestamp
