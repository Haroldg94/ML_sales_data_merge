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
formater = logging.Formatter('[%(asctime)s] - %(levelname)s - %(message)s')
fh = logging.FileHandler('data_processing.log', mode='w')
fh.setFormatter(formater)
logger.addHandler(fh)


def get_activities_df(df):
    filter_columns = ['Fecha de compra (date_created)',
                      'Identificador de producto (item_id)',
                      'Descripción de la operación (reason)',
                      'Código de referencia (external_reference)',
                      'SKU Producto (seller_custom_field)',
                      'Número de operación de Mercado Pago (operation_id)',
                      'Estado de la operación (status)',
                      'Detalle del estado de la operación (status_detail)',
                      'Tipo de operación (operation_type)',
                      'Valor del producto (transaction_amount)',
                      'Comisión por uso de plataforma de terceros (marketplace_fee)',
                      'Costo de envío (shipping_cost)',
                      'Descuento a tu contraparte (coupon_fee)',
                      'Monto recibido (net_received_amount)',
                      'Medio de pago (payment_type)',
                      'Monto devuelto (amount_refunded)',
                      'Número de venta en Mercado Libre (order_id)',
                      'Estado del envío (shipment_status)']
    # Filter the dataframe to get only the needed columns
    df = df[filter_columns]
    # Reduced name for the columns
    new_col_names = {col_name: re.findall(r'\((.*)\)', col_name)[0] for col_name in filter_columns}
    # Renaming the columns
    df.rename(columns=new_col_names, inplace=True)
    df.rename(columns={'seller_custom_field': 'SKU'}, inplace=True)
    # Setting the values of some columns to be positive
    df[['marketplace_fee', 'shipping_cost', 'coupon_fee']] = df[
        ['marketplace_fee', 'shipping_cost', 'coupon_fee']].apply(lambda x: -1 * x)
    return df


def populate_missing_fields(main_df, support_df):
    df_filter = (main_df['marketplace_fee'] == 0) & (main_df['operation_type'] != 'shipping')
    supp_df_cols = ['SOURCE_ID', 'FEE_AMOUNT', 'SETTLEMENT_NET_AMOUNT']
    main_df = main_df.merge(right=support_df.loc[support_df['TRANSACTION_TYPE'] != 'REFUND', supp_df_cols],
                            how='left', left_on='operation_id', right_on='SOURCE_ID')
    main_df.loc[df_filter, 'net_received_amount'] = main_df.loc[df_filter, 'SETTLEMENT_NET_AMOUNT']
    main_df.loc[df_filter, 'marketplace_fee'] = main_df.loc[df_filter, 'FEE_AMOUNT'] * -1

    return main_df


def add_taxes_col(df):
    df['taxes_head'] = df['transaction_amount'] - df['marketplace_fee'] - df['shipping_cost'] - df['coupon_fee'] - \
                       df['net_received_amount']
    return df


def main():
    logger.info('Start data processing program')
    working_path = os.getcwd()
    input_files_path = working_path + '/BI'

    files_names_start = {'activities-collection': 'xlsx',
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
        files_names_start_list = list(files_names_start.keys())
        for file in files_to_load:
            logger.debug(f'Processing {file} file')
            try:
                if file.split('.')[-1] == 'xlsx':
                    temp = pd.read_excel(os.path.join(input_files_path, file), engine='openpyxl')
                elif file.split('.')[-1] == 'csv':
                    temp = pd.read_csv(os.path.join(input_files_path, file))

                # Assigning the temp dataframe to the corresponding dataframe considering the filename
                if file.startswith(files_names_start_list[0]):
                    activities_collection = get_activities_df(temp)
                elif file.startswith(files_names_start_list[1]):
                    settlement_report = temp
                elif file.startswith(files_names_start_list[2]):
                    stock_general_full = temp
                elif file.startswith(files_names_start_list[3]):
                    ventas_co = temp
            except Exception as ex:
                logger.error(traceback.format_exc())

        populate_missing_fields(main_df=activities_collection, support_df=settlement_report)

        logger.debug('Populating the missing marketplace fees')
        activities_collection = populate_missing_fields(activities_collection, settlement_report)
        logger.debug('Adding the taxes column')
        activities_collection = add_taxes_col(activities_collection)
        activities_collection.to_excel('test.xlsx')

    logger.info('Data processing is done')


if __name__ == '__main__':
    main()
