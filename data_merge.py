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
    df['marketplace'] = 'Mercadolibre'
    df = df.astype({'external_reference': object, 'item_id': object, 'SKU': object, 'operation_id': object,
                    'order_id': object})
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


def generate_aux_data(df):
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


def do_archive(input_files_path, archive_path, file_date, file_name):
    destination_folder_path = os.path.join(archive_path, file_date)
    if not os.path.isdir(archive_path):
        os.mkdir(archive_path)

    if not os.path.isdir(destination_folder_path):
        os.mkdir(destination_folder_path)

    shutil.copy(os.path.join(input_files_path, file_name), destination_folder_path)
    #shutil.move(os.path.join(input_files_path, file_name), destination_folder_path)


def open_excel(excel_path):
    saved_data = pd.read_excel(excel_path)
    return saved_data


def indentify_new_sales(historical_df, new_df, src_col, trg_col):
    new_df = new_df.loc[~new_df[trg_col].isin(historical_df[src_col]), :]
    return new_df


def fix_refunded_sales(df):
    df.loc[df['amount_refunded'] != 0,
           ['marketplace_fee', 'shipping_cost', 'coupon_fee', 'net_received_amount', 'taxes_head']] = 0
    df.loc[df['amount_refunded'] != 0, 'amount_refunded'] = df.loc[df['amount_refunded'] != 0, 'transaction_amount']
    return df


def main():
    logger.info('Start data processing program')
    data_folder = 'BI'
    main_data_file = 'main_data.xlsx'
    consolidated_file = 'consolidated_data.xlsx'
    inventory_file = 'total_inventory.xlsx'
    archive_data = 'Archive'
    working_path = os.getcwd()
    input_files_path = os.path.join(working_path, data_folder)
    archive_path = os.path.join(input_files_path, archive_data)
    historical_path = os.path.join(working_path, main_data_file)
    consolidated_path = os.path.join(working_path, consolidated_file)
    inventory_path = os.path.join(working_path, inventory_file)
    c_activities = 0
    c_settle = 0
    c_sales = 0
    activities = False
    ventas = False
    settlement = False
    stock_casa = False
    stock_full = False
    archive = False

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
                         'Ventas_CO': 'xlsx',
                         'Inventario MELI (CASA)': 'xlsx'}

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
        if os.path.isfile(historical_path):
            logger.debug('Opening historical data')
            historical_df = open_excel(historical_path)
        else:
            historical_df = pd.DataFrame(columns=['date_created', 'item_id', 'reason', 'external_reference', 'SKU',
                                                  'operation_id', 'status', 'status_detail', 'operation_type',
                                                  'transaction_amount', 'marketplace_fee', 'shipping_cost',
                                                  'coupon_fee', 'net_received_amount', 'payment_type',
                                                  'amount_refunded', 'order_id', 'shipment_status', 'time_created',
                                                  'file_date', 'quantity', 'taxes_head', 'marketplace'])
        for file in files_to_load:
            logger.debug(f'Processing {file} file')
            try:
                if file.split('.')[-1] == 'xlsx':
                    if file.startswith(files_names_start_list[3]) or file.startswith(files_names_start_list[2]):
                        temp = pd.read_excel(os.path.join(input_files_path, file), engine='openpyxl', skiprows=3)
                    else:
                        temp = pd.read_excel(os.path.join(input_files_path, file), engine='openpyxl')
                elif file.split('.')[-1] == 'csv':
                    temp = pd.read_csv(os.path.join(input_files_path, file))

                # Assigning the temp dataframe to the corresponding dataframe considering the filename
                if file.startswith(files_names_start_list[0]):
                    file_date = datetime.strptime(re.findall(r'-([0-9]{14})-', file)[0], '%Y%m%d%H%M%S')
                    date = file_date.strftime('%Y%m%d')
                    temp = indentify_new_sales(historical_df, temp, 'external_reference',
                                               'Código de referencia (external_reference)')
                    if c_activities == 0:
                        activities_collection = get_activities_df(temp, file_date)
                    else:
                        activities_collection = pd.concat([activities_collection, get_activities_df(temp, file_date)],
                                                          axis=0)
                    c_activities += 1
                    activities = True
                    archive = True
                elif file.startswith(files_names_start_list[1]):
                    date = datetime.strptime(''.join(re.findall(r'-([0-9]{4})-([0-9]{2})-([0-9]{1,2})', file)[0]),
                                             '%Y%m%d').strftime('%Y%m%d')
                    temp = temp.astype({'SOURCE_ID': object, 'EXTERNAL_REFERENCE': object,
                                        'ORDER_ID': object, 'SHIPPING_ID': object})
                    temp = indentify_new_sales(historical_df, temp, 'operation_id', 'SOURCE_ID')
                    if c_settle == 0:
                        settlement_report = temp
                    else:
                        settlement_report = pd.concat([settlement_report, temp], axis=0)
                    c_settle += 1
                    settlement = True
                    archive = True
                elif file.startswith(files_names_start_list[2]):
                    date = datetime.strptime(''.join(re.findall(r'_([0-9]{1,2})-([0-9]{2})-([0-9]{4})_', file)[0]),
                                             '%d%m%Y').strftime('%Y%m%d')
                    temp.rename(columns={'Código ML': 'ml_code', 'ID de publicación': 'MCO'}, inplace=True)
                    temp = temp.astype({'MCO': object})
                    stock_general_full = temp
                    stock_full = True
                    archive = True
                elif file.startswith(files_names_start_list[3]):
                    date = re.findall(r'_([0-9]{1,2})_de_([a-z]{3,10})_de_([0-9]{4})', file)[0]
                    date = datetime.strptime(date[2]+month_dict[date[1]]+date[0], '%Y%m%d').strftime('%Y%m%d')
                    temp = temp.astype({'# de venta': object, '# de publicación': object})
                    if c_sales == 0:
                        ventas_co = temp
                    else:
                        ventas_co = pd.concat([ventas_co, temp], axis=0)
                    c_sales += 1
                    ventas = True
                    archive = True
                elif file.startswith(files_names_start_list[4]):
                    temp = temp.astype({'CÓD ML': object, '# Producto': object})
                    stock_casa_df = temp
                    stock_casa = True
                    archive = False

                # Moving the current file to an archive except for the house inventory file
                if archive:
                    logger.debug(f'Moving the file {file} to the archive')
                    do_archive(input_files_path, archive_path, date, file)

            except Exception as ex:
                logger.error(ex)
                logger.error(traceback.format_exc())

        try:
            logger.debug(f'There are {len(activities_collection)} records to be added')
            if len(activities_collection) > 0:
                if activities and settlement and ventas:
                    logger.debug('Populating the missing marketplace fees')
                    activities_collection = populate_missing_fields(activities_collection, settlement_report)
                    logger.debug('Adding the quantities sold for each product')
                    activities_collection = add_quantities(activities_collection, ventas_co)
                    logger.debug('Adding the taxes column')
                    activities_collection = add_taxes_col(activities_collection)
                    logger.debug('Fixing the refunded values')
                    activities_collection = fix_refunded_sales(activities_collection)
                    activities_collection['item_id'] = activities_collection['item_id'].apply(lambda x: x.strip('MCO'))
                    logger.debug('Generating Auxiliary File')
                    aux_data = generate_aux_data(activities_collection)

                    # Inserting new sales into the historical data files
                    main_data = pd.concat([historical_df, activities_collection], axis=0).reset_index(drop=True)
                    if os.path.isdir(consolidated_path):
                        historical_consolidated = open_excel(consolidated_path)
                    else:
                        historical_consolidated = pd.DataFrame(columns=['date_created', 'item_id', 'reason',
                                                                        'external_reference', 'SKU', 'operation_id',
                                                                        'status', 'status_detail', 'operation_type',
                                                                        'amount', 'payment_type', 'order_id',
                                                                        'shipment_status', 'time_created', 'file_date',
                                                                        'quantity', 'transaction_type', 'marketplace'
                                                                        ])
                    consolidated_data = pd.concat([historical_consolidated, aux_data], axis=0).reset_index(drop=True)
                    # Saving the files with the new data added
                    main_data.to_excel(historical_path, index=False, sheet_name='main')
                    consolidated_data.to_excel('consolidated_data.xlsx', index=False, sheet_name='consolidated')
                else:
                    logger.info('Some of the sales data are missing in the input files path')
            else:
                logger.info('There is no new data to add')

            if stock_casa and stock_full:
                logger.debug('Processing inventory files')
                inventory = stock_casa_df.merge(how='left',
                                                right=stock_general_full.loc[:, ['ml_code', 'Stock total almacenado']],
                                                left_on=['CÓD ML'],
                                                right_on=['ml_code']
                                                )
                inventory['Stock total almacenado'].fillna(value=0, inplace=True)
                inventory['Total'] = inventory['Stock total almacenado'] + inventory['INVENTARIO CASA']
                inventory.drop(columns=['ml_code'], inplace=True)
                inventory.to_excel(inventory_path, index=False)
            else:
                logger.info('Some of the inventory files is missing')

        except Exception as ex:
            logger.error(ex)
            logger.error(traceback.format_exc())
    else:
        logger.debug('There are no files to process')
    logger.info('Data processing is done')


if __name__ == '__main__':
    main()
