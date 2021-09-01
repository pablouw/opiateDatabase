from datetime import datetime as dt
from __init__ import spelling_dict
import re


def replace_comp_spelling(compound):
    for k, v in spelling_dict.items():
        if type(v) == list:
            if compound in v:
                compound = k
        else:
            if compound == v:
                compound = k

    return compound


def search_instrument(filename):
    if re.search('[xX][1-2]', filename):
        return filename
    else:
        print(f'No instument found (X1, X2) in {filename}. See filename format "YYYY-MM-DD_X1/2".')
        return False


def date_format(filename):
    datestring = re.compile('[0-9]{4}[-_][0-9]{2}[-_][0-9]{2}')
    if datestring.search(filename):
        split_character = "/"
        if "\\" in filename:
            split_character = "\\"
        filename_list = filename.split(split_character)
        for f in filename_list:
            if '.xml' in f:
                f = f.split(".")[0]
            if 'xml' in f:
                print(f'Please upload a valid XML file. {filename}')
                return False
            else:
                return f
    else:
        print(f'No valid date found in {filename}. Refer to filename format "YYYY-MM-DD_X1/2".')
        return False


def validate_date(date_string, date_list):
    try:
        valid_date = dt(year=int(date_list[0]), month=int(date_list[1]), day=int(date_list[2])).date()
        return str(valid_date)
    except ValueError as error:
        print(f"File: {date_string} isn't in a valid date. See error below, edit filename, and try again.")
        print("Error:", error)
        return False


def validate_filename(file_string):
    file_string = date_format(file_string)
    if not file_string:
        return False
    file_string = search_instrument(file_string)
    if not file_string:
        return False
    array = re.findall(r'[0-9]+', file_string)
    testname = validate_date(file_string, array[0:3])
    if testname:
        if len(array) == 4:
            testname += "_X" + array[3]
            return testname
        elif len(array) == 5:
            testname += "_X" + array[3] + "_p" + array[4]
            return testname
        elif len(array) >= 6:
            testname += "_X" + array[3] + "_p" + array[4]
            return testname
    else:
        return testname


def get_file_names(xmlfile):
    xmlfilename = xmlfile.split('.')[-2]
    xmlfilename = re.split(r' |/|\\', xmlfilename)[-1]
    mod_filename = validate_filename(xmlfilename)
    return xmlfilename, mod_filename
