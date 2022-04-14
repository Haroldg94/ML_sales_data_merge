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


def get_activities_df(df, file_date):
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
    df['date_created'] = pd.to_datetime(df['date_created'], format='%d/%m/%Y %H:%M:%S')
    df['time_created'] = df['date_created'].dt.time
    df['date_created'] = df['date_created'].dt.date
    df['file_date'] = file_date.date()
    return df


def get_col_idx(df, col):
    return df.columns.tolist().index(col)


def get_idx_list(df):
    return df.index.tolist()


def populate_missing_fields(main_df, support_df):
    # Populate the missing marketplace fee amount on the main_df using the support_df and fix the net received amount
    df_filter = (main_df['marketplace_fee'] == 0) & (main_df['operation_type'] != 'shipping')
    supp_df_cols = ['SOURCE_ID', 'FEE_AMOUNT', 'SETTLEMENT_NET_AMOUNT']
    main_df = main_df.merge(right=support_df.loc[support_df['TRANSACTION_TYPE'] != 'REFUND', supp_df_cols],
                            how='left', left_on='operation_id', right_on='SOURCE_ID')
    main_df.loc[df_filter, 'net_received_amount'] = main_df.loc[df_filter, 'SETTLEMENT_NET_AMOUNT']
    main_df.loc[df_filter, 'marketplace_fee'] = main_df.loc[df_filter, 'FEE_AMOUNT'] * -1
    # Populating the missing shipping cost paid by the seller
    seller_shipping_list = main_df.loc[main_df['operation_type'] == 'shipping', 'external_reference'].tolist()
    df1 = main_df.loc[main_df['external_reference'].isin(seller_shipping_list), :]
    df2 = df1.loc[df1['operation_type'] != 'shipping', :]
    # df with the dataframe in which I need to update the shipping cost
    shipping_to_update = df2.loc[~df2.duplicated(subset=['external_reference', 'operation_type', 'shipping_cost']), :]
    # df that have the shipping cost values
    new_shipping_val = main_df.loc[main_df['operation_type'] == 'shipping', :]
    # Add the new shipping cost
    main_df.iloc[get_idx_list(shipping_to_update),
                 get_col_idx(shipping_to_update, 'shipping_cost')] = main_df.iloc[get_idx_list(new_shipping_val),
                                                                                  get_col_idx(new_shipping_val,
                                                                                              'shipping_cost')]

    # Compute again the net received amount considering the shipping cost we just added
    main_df.iloc[get_idx_list(shipping_to_update),
                 get_col_idx(shipping_to_update,
                             'net_received_amount')] = main_df.iloc[get_idx_list(shipping_to_update),
                                                                    get_col_idx(shipping_to_update,
                                                                                'net_received_amount')] - \
                                                       main_df.iloc[get_idx_list(shipping_to_update),
                                                                    get_col_idx(shipping_to_update,
                                                                                'shipping_cost')]
    # Dropping the rows and columns that we don't need anymore
    main_df.drop(index=new_shipping_val.index.tolist(), inplace=True)
    main_df.drop(columns=supp_df_cols, inplace=True)
    # For the sales that have repeated the shipping_cost value we need to left just one for each sale
    main_df.loc[main_df.duplicated(subset=['order_id', 'shipping_cost'],
                                   keep='first') & (main_df['shipping_cost'] > 0), 'shipping_cost'] = 0

    return main_df


def add_taxes_col(df):
    df['taxes_head'] = df['transaction_amount'] - df['marketplace_fee'] - df['shipping_cost'] - df['coupon_fee'] - \
                       df['net_received_amount']
    return df


def add_quantities(main_df, support_df):
    # Merging our main dataframe with the sales dataframe to get the quantity sold in each sale
    main_df = main_df.merge(right=support_df.loc[:, ['# de venta', '# de publicación', 'Unidades']],
                            how='left',
                            left_on=['order_id', 'item_id'],
                            right_on=['# de venta', '# de publicación'])
    # Dropping the support columns
    main_df.drop(columns=['# de venta', '# de publicación'], inplace=True)
    # Renaming the quantity column and filling the null values
    main_df.rename(columns={'Unidades': 'quantity'}, inplace=True)
    main_df['quantity'].fillna(value=0, inplace=True)

    return main_df


def generate_aux_file(df):
    tr_list = ['transaction_amount', 'marketplace_fee', 'shipping_cost', 'coupon_fee', 'net_received_amount',
               'amount_refunded', 'taxes_head']
    df1 = df.drop(columns=tr_list[1:])
    df1.rename(columns={tr_list[0]: 'amount'}, inplace=True)
    df1['transaction_type'] = tr_list[0]
    df2 = df.drop(columns=tr_list[0])
    df2 = df2.drop(columns=tr_list[2:])
    df2.rename(columns={tr_list[1]: 'amount'}, inplace=True)
    df2['transaction_type'] = tr_list[1]
    df3 = df.drop(columns=tr_list[0:2])
    df3 = df3.drop(columns=tr_list[3:])
    df3.rename(columns={tr_list[2]: 'amount'}, inplace=True)
    df3['transaction_type'] = tr_list[2]
    df4 = df.drop(columns=tr_list[0:3])
    df4 = df4.drop(columns=tr_list[4:])
    df4.rename(columns={tr_list[3]: 'amount'}, inplace=True)
    df4['transaction_type'] = tr_list[3]
    df5 = df.drop(columns=tr_list[0:4])
    df5 = df5.drop(columns=tr_list[5:])
    df5.rename(columns={tr_list[4]: 'amount'}, inplace=True)
    df5['transaction_type'] = tr_list[4]
    df6 = df.drop(columns=tr_list[0:5])
    df6 = df6.drop(columns=tr_list[6])
    df6.rename(columns={tr_list[5]: 'amount'}, inplace=True)
    df6['transaction_type'] = tr_list[5]
    df7 = df.drop(columns=tr_list[0:6])
    df7.rename(columns={tr_list[6]: 'amount'}, inplace=True)
    df7['transaction_type'] = tr_list[6]

    final_df = pd.concat([df1, df2, df3, df4, df5, df6, df7], axis=0)
    return final_df


def main():
    logger.info('Start data processing program')
    working_path = os.getcwd()
    input_files_path = working_path + '/BI'
    archive_path = input_files_path + '/archive'

    month_dict = {
        'enero': '01',
        'febrero': '02',
        'marzo': '03',
        'abril': '04',
        'mayo': '05',
        'junio': '06',
        'julio': '07',
        'agosto': '08',
        'septiembre': '09',
        'octubre': '10',
        'noviembre': '11',
        'diciembre': '12'
    }

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
                    if file.startswith(files_names_start_list[3]):
                        temp = pd.read_excel(os.path.join(input_files_path, file), engine='openpyxl', skiprows=3)
                    else:
                        temp = pd.read_excel(os.path.join(input_files_path, file), engine='openpyxl')
                elif file.split('.')[-1] == 'csv':
                    temp = pd.read_csv(os.path.join(input_files_path, file))

                # Assigning the temp dataframe to the corresponding dataframe considering the filename
                if file.startswith(files_names_start_list[0]):
                    file_date = datetime.strptime(re.findall(r'-([0-9]{14})-', file)[0], '%Y%m%d%H%M%S')
                    date = file_date.strftime('%Y%m%d')
                    activities_collection = get_activities_df(temp, file_date)
                    activities = True
                elif file.startswith(files_names_start_list[1]):
                    date = ''.join(re.findall(r'-([0-9]{4})-([0-9]{2})-([0-9]{2})', file)[0])
                    settlement_report = temp
                    settlement = True
                elif file.startswith(files_names_start_list[2]):
                    date = datetime.strptime(''.join(re.findall(r'_([0-9]{2})-([0-9]{2})-([0-9]{4})_', file)[0]),
                                             '%d%m%Y').strftime('%Y%m%d')
                    stock_general_full = temp
                    stock_full = True
                elif file.startswith(files_names_start_list[3]):
                    date = re.findall(r'_([0-9]{2})_de_([a-z]{3,10})_de_([0-9]{4})',file)[0]
                    date = date[2] + month_dict[date[1]] + date[0]
                    ventas_co = temp
                    ventas = True
            except Exception as ex:
                logger.error(traceback.format_exc())

        try:
            logger.debug('Populating the missing marketplace fees')
            activities_collection = populate_missing_fields(activities_collection, settlement_report)
            logger.debug('Adding the quantities sold for each product')
            activities_collection = add_quantities(activities_collection, ventas_co)
            logger.debug('Adding the taxes column')
            activities_collection = add_taxes_col(activities_collection)
            logger.debug('Generating Auxiliary File')
            aux_file = generate_aux_file(activities_collection)
            activities_collection.to_excel('main_data.xlsx', index=False)
            aux_file.to_excel('consolidated_data.xlsx', index=False)

        except Exception as ex:
            logger.error(ex)
            logger.error(traceback.format_exc())
    else:
        logger.debug('There are no files to process')
    logger.info('Data processing is done')


if __name__ == '__main__':
    main()
