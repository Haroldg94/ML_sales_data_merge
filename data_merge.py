import pandas as pd
import os
import re
from fnmatch import fnmatch
from datetime import datetime
import traceback
import logging
import shutil

# Logger configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formater = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh = logging.FileHandler('data_processing.log', mode='w')
fh.setFormatter(formater)
logger.addHandler(fh)


def main():
    logger.info('Start data processing program')
    working_path = os.getcwd()
    input_files_path = working_path + '/BI'

    files_names_start = {'activities-collection': 'csv',
                         'settlement-report': 'xlsx',
                         'Stock_general_Full': 'xlsx',
                         'Ventas_CO': 'xlsx'}

    # Getting the files in the input file directory
    files_in_path = [f for f in os.listdir(input_files_path) if os.path.isfile(os.path.join(input_files_path, f))]
    print(f'files in the path: {files_in_path}')
    # Getting the files to load from the files in the input file path
    files_to_load = [f for f in files_in_path for n in files_names_start.keys()
                     if f.startswith(n) and (fnmatch(f, f'*.{files_names_start[n]}'))]
    print(f'files_to_load: {files_to_load}')

    logger.debug(f'Found {len(files_to_load)} files to process')
    if len(files_to_load) > 0:
        for file in files_to_load:
            logger.debug(f'Processing {file} file')
            try:
                if file.split('.')[-1] == 'xlsx':
                    temp = pd.read_excel(os.path.join(input_files_path, file), engine='openpyxl')
                if file.split('.')[-1] == 'csv':
                    temp = pd.read_csv(os.path.join(input_files_path, file))


            except Exception as ex:
                logger.error(traceback.format_exc())
    logger.info('Data processing is done')


if __name__ == '__main__':
    main()
