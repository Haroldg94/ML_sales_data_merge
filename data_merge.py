import pandas as pd
import os
import re
from fnmatch import fnmatch


def main():
    working_path = os.getcwd()
    input_files_path = working_path + '/BI'

    files_names_start = ['activities-collection',
                         'settlement-report',
                         'Stock_general_Full',
                         'Ventas_CO']

    # Getting the files in the input file directory
    files_in_path = [f for f in os.listdir(input_files_path) if os.path.isfile(os.path.join(input_files_path, f))]
    print(f'files in the path: {files_in_path}')
    # Getting the files to load from the files in the input file path
    files_to_load = [f for f in files_in_path for n in files_names_start
                     if f.startswith(n) and (fnmatch(f, '*.csv') or fnmatch(f, '*.xlsx'))]
    print(f'files_to_load: {files_to_load}')

    if len(files_to_load) > 0:
        for file in files_to_load:
            try:
                if file.split('.')[-1] == 'xlsx':
                    print(f'{file} is a .xlsx file')
                if file.split('.')[-1] == 'xls':
                    print(f'{file} is a .xls file')
            except:
                pass


if __name__ == '__main__':
    main()
