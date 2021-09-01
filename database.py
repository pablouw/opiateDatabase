from sqlalchemy.sql.expression import bindparam
from datetime import datetime as dt
import sqlalchemy
import sys


def create_db(engine):
    metadata = sqlalchemy.MetaData(engine)
    # Create tables with the appropriate Columns
    calibration = sqlalchemy.Table('calibration',
                                   metadata,
                                   sqlalchemy.Column('calibration_pkey',
                                                     sqlalchemy.Integer,
                                                     primary_key=True,
                                                     nullable=False),
                                   sqlalchemy.Column('xml_file', sqlalchemy.String),
                                   sqlalchemy.Column('modified_filename', sqlalchemy.String),
                                   sqlalchemy.Column('timestamp', sqlalchemy.DateTime),
                                   sqlalchemy.Column('compound', sqlalchemy.String),
                                   sqlalchemy.Column('curve_type', sqlalchemy.String),
                                   sqlalchemy.Column('origin', sqlalchemy.String, nullable=True),
                                   sqlalchemy.Column('weighting', sqlalchemy.String, nullable=True),
                                   sqlalchemy.Column('slope', sqlalchemy.String, nullable=True),
                                   sqlalchemy.Column('rsquared', sqlalchemy.Float, nullable=True),
                                   sqlalchemy.Column('cc', sqlalchemy.Float, nullable=True),
                                   sqlalchemy.Column('int_std_id', sqlalchemy.Integer),
                                   sqlalchemy.Column('int_std', sqlalchemy.String))

    results = sqlalchemy.Table('results', metadata,
                               sqlalchemy.Column('results_pkey',
                                                 sqlalchemy.Integer,
                                                 primary_key=True,
                                                 nullable=False),
                               sqlalchemy.Column('xml_file', sqlalchemy.String),
                               sqlalchemy.Column('modified_filename', sqlalchemy.String),
                               sqlalchemy.Column('lc_batch', sqlalchemy.String),
                               sqlalchemy.Column('injection_time', sqlalchemy.DateTime),
                               sqlalchemy.Column('vial', sqlalchemy.String),
                               sqlalchemy.Column('sample', sqlalchemy.String),
                               sqlalchemy.Column('sample_id', sqlalchemy.Integer),
                               sqlalchemy.Column('sample_type', sqlalchemy.String),
                               sqlalchemy.Column('compound', sqlalchemy.String),
                               sqlalchemy.Column('compound_id', sqlalchemy.Integer),
                               sqlalchemy.Column('peak_area', sqlalchemy.Float, nullable=True),
                               sqlalchemy.Column('concentration', sqlalchemy.Float, nullable=True),
                               sqlalchemy.Column('RT', sqlalchemy.Float, nullable=True),
                               sqlalchemy.Column('SN', sqlalchemy.Float, nullable=True),
                               sqlalchemy.Column('confirming_ion_area', sqlalchemy.Float, nullable=True),
                               sqlalchemy.Column('int_std_id', sqlalchemy.Integer),
                               sqlalchemy.Column('int_std', sqlalchemy.String),
                               sqlalchemy.Column('is_peak_area', sqlalchemy.Float, nullable=True),
                               sqlalchemy.Column('is_RT', sqlalchemy.Float, nullable=True),
                               sqlalchemy.Column('is_SN', sqlalchemy.Float, nullable=True),
                               sqlalchemy.Column('is_confirming_ion_area', sqlalchemy.Float, nullable=True))

    batch = sqlalchemy.Table('batch', metadata,
                             sqlalchemy.Column('batch_pkey',
                                               sqlalchemy.Integer,
                                               primary_key=True,
                                               nullable=False),
                             sqlalchemy.Column('xml_file', sqlalchemy.String),
                             sqlalchemy.Column('modified_filename', sqlalchemy.String),
                             sqlalchemy.Column('timestamp', sqlalchemy.DateTime),
                             sqlalchemy.Column('instrument', sqlalchemy.String),
                             sqlalchemy.Column('numb_samples', sqlalchemy.Integer))
    # Implement the creation
    metadata.create_all(engine)
    return [calibration, batch, results]


def execute_calibration(db_table, value_dictionary):
    cstmt = db_table.update().where(db_table.c.calibration_pkey == bindparam("_id")).values(value_dictionary)
    return cstmt


def execute_result(db_table, value_dictionary):
    rstmt = db_table.update().where(db_table.c.results_pkey == bindparam("_id")).values(value_dictionary)
    return rstmt


def get_table_schema(table):
    if str(table.name) == 'batch':
        values = {
            'xml_file': bindparam('xml_file'),
            'modified_filename': bindparam('modified_filename'),
            'timestamp': bindparam('timestamp'),
            'instrument': bindparam('instrument'),
            'numb_samples': bindparam('numb_samples')
        }
        stmt = table.update().where(table.c.batch_pkey == bindparam("_id")).values(values)
        return stmt
    if str(table.name) == 'calibration':
        values = {
            'xml_file': bindparam('xml_file'),
            'modified_filename': bindparam('modified_filename'),
            'timestamp': bindparam('timestamp'),
            'compound': bindparam('compound'),
            'curve_type': bindparam('curve_type'),
            'origin': bindparam('origin'),
            'weighting': bindparam('weighting'),
            'slope': bindparam('slope'),
            'rsquared': bindparam('rsquared'),
            'cc': bindparam('cc'),
            'int_std_id': bindparam('int_std_id'),
            'int_std': bindparam('int_std')
        }
        stmt = execute_calibration(table, values)
        return stmt
    if str(table.name) == 'results':
        values = {
            'xml_file': bindparam('xml_file'),
            'modified_filename': bindparam('modified_filename'),
            'lc_batch': bindparam('lc_batch'),
            'injection_time': bindparam('injection_time'),
            'vial': bindparam('vial'),
            'sample': bindparam('sample'),
            'sample_id': bindparam('sample_id'),
            'sample_type': bindparam('sample_type'),
            'compound': bindparam('compound'),
            'compound_id': bindparam('compound_id'),
            'peak_area': bindparam('peak_area'),
            'concentration': bindparam('concentration'),
            'RT': bindparam('RT'),
            'SN': bindparam('SN'),
            'confirming_ion_area': bindparam('confirming_ion_area'),
            'int_std_id': bindparam('int_std_id'),
            'int_std': bindparam('int_std'),
            'is_peak_area': bindparam('is_peak_area'),
            'is_RT': bindparam('is_RT'),
            'is_SN': bindparam('is_SN'),
            'is_confirming_ion_area': bindparam('is_confirming_ion_area'),
        }
        stmt = execute_result(table, values)
        return stmt


def execute_table_changes(engine_, db_table, replacement_data):
    statement = get_table_schema(db_table)
    for item in replacement_data:
        engine_.execute(statement, [item])


def execute_batch(engine_, db_table, replacement_data):
    bstmt = get_table_schema(db_table)
    engine_.execute(bstmt, [replacement_data])


def grab_entries_from_db(engine, file, table):
    s = sqlalchemy.sql.select([table]).where(table.c.xml_file == file)
    result = engine.execute(s)
    res = result.fetchall()
    return res


def check_matching_file_entry_length(engine, tables, xml_file, data_to_replace):
    calibrator_length, sample_number = len(data_to_replace[0]), data_to_replace[1][4]
    cal_table, batch_table, results_table = tables
    cal_table_data = grab_entries_from_db(engine, xml_file, cal_table)
    batch_table_data = grab_entries_from_db(engine, xml_file, batch_table)
    batch_length = batch_table_data[0][5]
    results_table_data = grab_entries_from_db(engine, xml_file, results_table)
    if (len(cal_table_data) == calibrator_length) and (batch_length == sample_number):
        return [cal_table_data, batch_table_data, results_table_data]
    else:
        return [False, False, False]


def check_if_in_db(engine, xml_file, calibration):
    s = sqlalchemy.sql.select([calibration.c.xml_file]).where(calibration.c.xml_file == xml_file)
    result = engine.execute(s)
    res = result.fetchone()
    if res:
        return True
    else:
        return False


def add_to_calibration_table(df, engine):
    result, = engine.execute('SELECT max(calibration_pkey) FROM calibration')
    result = result[0]
    if result is None:
        result = -1
    result = result + 1
    df.index += result
    df.to_sql('calibration', engine, if_exists="append", index_label='calibration_pkey')


def add_to_result_table(df, engine):
    result, = engine.execute('SELECT max(results_pkey) FROM results')
    result = result[0]
    if result is None:
        result = -1
    result = result + 1
    df.index += result
    df.to_sql('results', engine, if_exists='append', index_label='results_pkey')


def add_to_batch_table(batch_data, engine, batch):
    max_id, = engine.execute('SELECT max(batch_pkey) FROM batch')
    max_id = max_id[0]
    if max_id is None:
        max_id = -1
    new_id = max_id + 1
    ins = batch.insert().values(
                                batch_pkey=new_id,
                                xml_file=batch_data[0],
                                modified_filename=batch_data[1],
                                timestamp=dt.strptime(batch_data[2], '%Y-%m-%d %H:%M:%S'),
                                instrument=batch_data[3],
                                numb_samples=batch_data[4])
    engine.execute(ins)


def make_engine(db_type):
    if db_type == 'postgres':
        # change to fit actual path
        engine = sqlalchemy.create_engine('postgresql://username:password@localhost/opiates')
    elif db_type == 'sqlite':
        # change to fit actual path
        engine = sqlalchemy.create_engine('sqlite:////Path/to/file/opiates.db')
    else:
        print('No recognizied database type selected. Will terminate process.')
        sys.exit()
    table_list = create_db(engine)
    return engine, table_list
