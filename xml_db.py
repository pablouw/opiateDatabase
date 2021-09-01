from database import make_engine, check_if_in_db, add_to_calibration_table, add_to_result_table, add_to_batch_table
from verify_data import get_file_names
from process_data import collect_and_replace_data_db, extract_data, read_xml
import argparse
import os
import sys


def file_addition(file, db_type, replace):
    if not file.endswith(".xml"):
        print(f'{file} is not an xml file. Please enter an xml file')
        sys.exit()
    engine, tables_list = make_engine(db_type)
    file, mod_file = get_file_names(file)
    if not mod_file:
        print(f'File format invalid ({file}). Not added to db.')
        sys.exit()
    calibration = tables_list[0]
    db_decision = check_if_in_db(engine, file, calibration)

    if db_decision and replace:
        complete = collect_and_replace_data_db(engine, file, mod_file, tables_list)
        if complete:
            print(f'Successfully replaced {file} \n')
        else:
            print(f"{file} not replaced. \n")
    elif db_decision and not replace:
        print(f'{file} found in db, not added to db. \n')
        sys.exit()
    elif not db_decision and replace:
        print('You have selected to replace a file that does not exist in the db.')
        print('Please check the filename and try again. \n')
        sys.exit()
    else:
        print(f'{file} not in db. Processing...')
        timestamp, root = read_xml(file)
        df_calib, batch_data, batch_df = extract_data(file, mod_file, timestamp, root)
        if df_calib is None:
            sys.exit()
        add_to_calibration_table(df_calib, engine)
        add_to_result_table(batch_df, engine)
        add_to_batch_table(batch_data, engine, tables_list[1])
        print('Done')


def directory_addition(directory, db_type):
    engine, tables_list = make_engine(db_type)
    for file in os.listdir(directory):
        if not file.endswith(".xml"):
            continue
        file, mod_file = get_file_names(file)
        if not mod_file:
            # print(f'File format invalid ({file}). Not added to db.')
            continue
        calibration = tables_list[0]
        db_decision = check_if_in_db(engine, file, calibration)
        if db_decision:
            print(f'{file} found in db, not added to db \n')
            continue
        print(f'{file} not in db. Processing... \n')
        path_file = os.path.join(directory, file)
        timestamp, root = read_xml(path_file)
        df_calib, batch_data, batch_df = extract_data(file, mod_file, timestamp, root)
        if df_calib is None:
            continue
        add_to_calibration_table(df_calib, engine)
        add_to_result_table(batch_df, engine)
        add_to_batch_table(batch_data, engine, tables_list[1])
    print('Done')


def check_input(args):
    uinput = args.user_input
    db = args.db_type
    replace = args.replace
    if os.path.isfile(uinput):
        file_addition(uinput, db, replace)
    elif os.path.isdir(uinput):
        if replace:
            print('Replacing files within a directory is not supported.')
            print('Please replace one file at a time.')
            sys.exit()
        directory_addition(uinput, db)
    else:
        print('Please place valid input (file or directory).')
        sys.exit()



def parse_arguments():
    parser = argparse.ArgumentParser(description="Place opiate XML files into SQL tables.")

    parser.add_argument('user_input', type=str,
                        help='The path to the XML file or directory with the XML files.')

    parser.add_argument('-db', '--database', type=str, choices=['postgres', 'sqlite'], default='postgres',
                        dest="db_type", help='Choose between a PostgreSQL or SQLite database')
    parser.add_argument('-re', '--replace', action='store_true', dest='replace',
                        help='Replace file X with the a new one of the same name.')
    parser.set_defaults(func=check_input)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    parse_arguments()
