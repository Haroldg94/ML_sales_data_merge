import pandas as pd
import os
import re
from fnmatch import fnmatch
from datetime import datetime
from datetime import timedelta
from datetime import date
import traceback
import logging
import shutil
import numpy as np

# Logger configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formater = logging.Formatter('[%(asctime)s] - %(levelname)s - %(message)s')
fh = logging.FileHandler('data_processing.log')# , mode='w')
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
    df = df[~df['date_created'].isnull()]
    return df


def get_col_idx(df, col):
    return df.columns.tolist().index(col)


def get_idx_list(df):
    return df.index.tolist()


def populate_missing_fields(main_df, support_df):
    # Populate the missing marketplace fee amount on the main_df using the support_df and fix the net received amount
    supp_df_cols = ['SOURCE_ID', 'TRANSACTION_AMOUNT', 'TAXES_AMOUNT', 'PACK_ID', 'MKP_FEE_AMOUNT']
    main_refunded = main_df[main_df['status'] == 'refunded']
    main_df = main_df[main_df['status'] != 'refunded'].merge(right=support_df[supp_df_cols],
                                                             how='left', left_on='operation_id', right_on='SOURCE_ID')
    # Renaming some fields
    rename_dict = {'transaction_amount': 'sale_amount',
                   'TRANSACTION_AMOUNT': 'transaction_amount',
                   'TAXES_AMOUNT': 'taxes_amount',
                   'PACK_ID': 'pack_id'
                   }
    main_df.rename(columns=rename_dict, inplace=True)

    # Process to populate shipping cost that came in a different row from the sale
    shipping_df = main_df.loc[main_df['operation_type'] == 'shipping', ['external_reference', 'shipping_cost']]
    # Dataframe to count the number of sales in which we need to divide the shipping cost
    rows_filter = (main_df['external_reference'].isin(shipping_df['external_reference'].values.tolist())
                   ) & (main_df['operation_type'] != 'shipping')
    count_df = main_df.loc[rows_filter, ['external_reference']].groupby(by=['external_reference']
                                                                        ).agg(count=('external_reference', 'count')
                                                                              ).reset_index()
    shipping_df = shipping_df.merge(right=count_df, how='left', on='external_reference')
    shipping_df['count'].fillna(value=1, inplace=True)
    shipping_df['shipping_cost'] = shipping_df['shipping_cost'] / shipping_df['count']
    main_df = main_df.merge(right=shipping_df, on='external_reference', how='left')

    rows_filter_2 = (main_df['external_reference'].isin(shipping_df['external_reference'].values.tolist())
                   ) & (main_df['operation_type'] != 'shipping')
    main_df.loc[rows_filter_2, 'shipping_cost_x'] = main_df.loc[rows_filter_2, 'shipping_cost_y']
    main_df = main_df[main_df['operation_type'] != 'shipping']
    # Fixing amounts
    main_df['taxes_amount'] = main_df['taxes_amount'] * -1
    rows_filter_3 = main_df['marketplace_fee'] == 0
    main_df.loc[rows_filter_3, 'marketplace_fee'] = main_df.loc[rows_filter_3, 'MKP_FEE_AMOUNT'] * -1

    # Dropping columns that we don´t need and renaming useful columns
    main_df.drop(columns=['shipping_cost_y', 'count', 'MKP_FEE_AMOUNT', 'SOURCE_ID'], inplace=True)
    main_df.rename(columns={'shipping_cost_x': 'shipping_cost_by_seller'}, inplace=True)

    return main_df, main_refunded


def add_shipping_cost_by_customer(df):
    rows_filter = (~df['transaction_amount'].isnull())
    df.loc[rows_filter, 'shipping_cost_by_customer'] = df.loc[rows_filter, 'transaction_amount'] - \
                                                       df.loc[rows_filter, 'sale_amount']
    values = {'shipping_cost_by_seller': 0, 'shipping_cost_by_customer': 0, 'marketplace_fee': 0, 'taxes_amount': 0}
    df.fillna(value=values, inplace=True)
    return df


def calculate_net_received_amount(df):
    df_filter = (~df['transaction_amount'].isnull()) & (df['transaction_amount'] != 0)
    df.loc[df_filter, 'net_received_amount'] = df.loc[df_filter, 'transaction_amount'] - \
                                               df.loc[df_filter, 'marketplace_fee'] - \
                                               df.loc[df_filter, 'shipping_cost_by_seller'] - \
                                               df.loc[df_filter, 'shipping_cost_by_customer'] - \
                                               df.loc[df_filter, 'coupon_fee'] - \
                                               df.loc[df_filter, 'taxes_amount']
    # Fill the null transaction amounts
    df_filter_2 = df['transaction_amount'].isnull()
    df.loc[df_filter_2, 'transaction_amount'] = df.loc[df_filter_2, 'net_received_amount'] + \
                                                df.loc[df_filter_2, 'marketplace_fee'] + \
                                                df.loc[df_filter_2, 'shipping_cost_by_seller'] + \
                                                df.loc[df_filter_2, 'shipping_cost_by_customer'] + \
                                                df.loc[df_filter_2, 'coupon_fee'] + \
                                                df.loc[df_filter_2, 'taxes_amount']
    return df


def remove_cancelled_sales(df, cancelled_path, dtypes):
    exclude_list = ['cancelled', 'rejected', 'pending']
    cancelled_filter = df['status'].isin(exclude_list)
    cancelled_df = df[cancelled_filter]
    if os.path.isfile(cancelled_path):
        logger.debug('Opening historical data of cancelled sales')
        cancelled_historical = open_excel(cancelled_path, dtypes=dtypes)
    else:
        cancelled_historical = pd.DataFrame(columns=['date_created', 'item_id', 'reason', 'external_reference', 'SKU',
                                                     'operation_id', 'status', 'status_detail', 'operation_type',
                                                     'sale_amount', 'marketplace_fee', 'shipping_cost_by_seller',
                                                     'coupon_fee', 'net_received_amount', 'payment_type',
                                                     'amount_refunded', 'order_id', 'shipment_status',
                                                     'time_created', 'file_date',
                                                     'SOURCE_ID', 'transaction_amount', 'taxes_amount', 'pack_id',
                                                     'shipping_cost_by_customer'])

    cancelled_data = pd.concat([cancelled_historical, cancelled_df], axis=0).reset_index(drop=True)
    cancelled_data.to_excel(cancelled_path, index=False, sheet_name='cancelled')

    # main_df without cancelled and rejected sales
    df = df[~cancelled_filter]
    return df


def add_refunded_sales(df, refund_df):
    # Prepare the refunded dataframe to concatenate it with the main_df_wr dataframe
    refund_df.rename(columns={'shipping_cost': 'shipping_cost_by_seller'}, inplace=True)

    refund_df = refund_df.assign(sale_amount=0, taxes_amount=0, pack_id=np.nan, shipping_cost_by_customer=0)
    df = pd.concat([df, refund_df], axis=0)
    df.sort_values(by=['file_date', 'date_created'], inplace=True)
    return df


def add_quantities_marketplace(main_df, sales_df):
    # Merging our main dataframe with the sales dataframe to get the quantity sold in each sale
    main_df = main_df.merge(right=sales_df.loc[:, ['# de venta', 'Unidades', 'Canal de venta']],
                            how='left',
                            left_on=['order_id'],
                            right_on=['# de venta'])
    # Dropping the support columns
    main_df.drop(columns=['# de venta'], inplace=True)
    # Renaming the quantity column and filling the null values
    main_df.rename(columns={'Unidades': 'quantity', 'Canal de venta': 'marketplace'}, inplace=True)
    main_df['quantity'].fillna(value=0, inplace=True)

    # Now using the pack_id as the key to join the dataframes
    sales_filter = (~sales_df['Unidades'].isnull()) & (sales_df['Unidades'] != 0)
    main_df = main_df.merge(right=sales_df.loc[sales_filter, ['# de venta', 'Unidades', 'Canal de venta']],
                            how='left',
                            left_on=['pack_id'],
                            right_on=['# de venta'])
    main_df.loc[main_df['quantity'] == 0, 'marketplace'] = main_df.loc[main_df['quantity'] == 0, 'Canal de venta']
    main_df.loc[main_df['quantity'] == 0, 'quantity'] = main_df.loc[main_df['quantity'] == 0, 'Unidades']
    # Dropping the support columns
    main_df.drop(columns=['# de venta', 'Unidades', 'Canal de venta'], inplace=True)
    fill_values = {'quantity': 0, 'marketplace': 'Mercado Libre'}
    main_df.fillna(value=fill_values, inplace=True)

    return main_df


def generate_aux_data(df):
    tr_list = ['transaction_amount', 'sale_amount', 'marketplace_fee', 'shipping_cost_by_seller',
               'shipping_cost_by_customer', 'coupon_fee', 'net_received_amount', 'amount_refunded', 'taxes_amount',
               'product_cost']
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
    df6 = df6.drop(columns=tr_list[6:])
    df6.rename(columns={tr_list[5]: 'amount'}, inplace=True)
    df6['transaction_type'] = tr_list[5]
    df7 = df.drop(columns=tr_list[0:6])
    df7 = df7.drop(columns=tr_list[7:])
    df7.rename(columns={tr_list[6]: 'amount'}, inplace=True)
    df7['transaction_type'] = tr_list[6]
    df8 = df.drop(columns=tr_list[0:7])
    df8 = df8.drop(columns=tr_list[8:])
    df8.rename(columns={tr_list[7]: 'amount'}, inplace=True)
    df8['transaction_type'] = tr_list[7]
    df9 = df.drop(columns=tr_list[0:8])
    df9 = df9.drop(columns=tr_list[9:])
    df9.rename(columns={tr_list[8]: 'amount'}, inplace=True)
    df9['transaction_type'] = tr_list[8]
    df10 = df.drop(columns=tr_list[0:9])
    df10.rename(columns={tr_list[9]: 'amount'}, inplace=True)
    df10['transaction_type'] = tr_list[9]

    final_df = pd.concat([df1, df2, df3, df4, df5, df6, df7, df8, df9,df10], axis=0)
    final_df = final_df[final_df['amount'] != 0]
    final_df.sort_values(by=['file_date', 'date_created', 'time_created'], inplace=True)
    return final_df


def do_archive(input_files_path, archive_path, file_date, file_name):
    destination_folder_path = os.path.join(archive_path, file_date)
    if not os.path.isdir(archive_path):
        os.mkdir(archive_path)

    if not os.path.isdir(destination_folder_path):
        os.mkdir(destination_folder_path)

    if os.path.isfile(os.path.join(destination_folder_path, file_name)):
        logger.debug(f'The file "{file_name}" already exist in the destination folder')
        logger.debug(f'overwriting file "{file_name}"...')
        os.remove(os.path.join(destination_folder_path, file_name))

    #shutil.copy(os.path.join(input_files_path, file_name), destination_folder_path)
    shutil.move(os.path.join(input_files_path, file_name), destination_folder_path)


def open_excel(excel_path, skiprows=0, dtypes=None):
    if dtypes is None:
        saved_data = pd.read_excel(excel_path, engine='openpyxl', skiprows=skiprows)
    else:
        saved_data = pd.read_excel(excel_path, engine='openpyxl', skiprows=skiprows, dtype=dtypes)
    return saved_data


def indentify_new_sales(historical_df, new_df, src_col, trg_col):
    new_df = new_df.loc[~new_df[trg_col].isin(historical_df[src_col]), :]
    return new_df


def fix_refunded_sales(df):
    # zero out some fields
    cols_to_zero = ['sale_amount', 'marketplace_fee', 'shipping_cost_by_seller', 'shipping_cost_by_customer',
                    'coupon_fee', 'net_received_amount', 'taxes_amount']
    df.loc[df['amount_refunded'] != 0, cols_to_zero] = 0
    # Fix transaction amount to match the amount refunded
    df.loc[df['amount_refunded'] != 0, 'transaction_amount'] = df.loc[df['amount_refunded'] != 0, 'amount_refunded']
    return df


def data_aggregation(df):
    group_cols = ['order_id', 'SKU', 'reason', 'item_id', 'external_reference', 'marketplace', 'status',
                  'status_detail', 'operation_type', 'shipment_status', 'pack_id']
    df = df.groupby(by=group_cols, dropna=False).agg(date_created=('date_created', 'max'),
                                                     time_created=('time_created', 'max'),
                                                     transaction_amount=('transaction_amount', 'sum'),
                                                     sale_amount=('sale_amount', 'sum'),
                                                     marketplace_fee=('marketplace_fee', 'sum'),
                                                     shipping_cost_by_seller=('shipping_cost_by_seller', 'sum'),
                                                     shipping_cost_by_customer=('shipping_cost_by_customer', 'sum'),
                                                     coupon_fee=('coupon_fee', 'sum'),
                                                     taxes_amount=('taxes_amount', 'sum'),
                                                     net_received_amount=('net_received_amount', 'sum'),
                                                     payment_type=('payment_type', ','.join),
                                                     amount_refunded=('amount_refunded', 'sum'),
                                                     operation_id=('operation_id', ','.join),
                                                     quantity=('quantity', 'mean'),
                                                     file_date=('file_date', 'max')
                                                     ).reset_index()
    df.sort_values(by=['file_date', 'date_created', 'time_created'], inplace=True)
    return df


def add_product_cost(main_df, cost_df):
    main_df = main_df.merge(right=cost_df,
                            how='left',
                            left_on='item_id',
                            right_on='# Publicacion')
    df_filter = main_df['date_created'] >= date(2023, 6, 1)
    main_df.loc[df_filter, 'product_cost'] = main_df.loc[df_filter, 'quantity'] * main_df.loc[df_filter, 'Total costo COP']
    main_df.drop(columns=['# Publicacion', 'Total costo COP'], inplace=True)
    return main_df


def import_file(file, files_names_start_list, input_files_path, dtypes=None):
    if dtypes is None:
        if file.split('.')[-1] == 'xlsx':
            if file.startswith(files_names_start_list[2]):
                import_df = open_excel(os.path.join(input_files_path, file), skiprows=3)
            elif file.startswith(files_names_start_list[3]):
                import_df = open_excel(os.path.join(input_files_path, file), skiprows=2)
                if '# de venta' not in import_df.columns:
                    import_df = open_excel(os.path.join(input_files_path, file), skiprows=3)
            else:
                import_df = open_excel(os.path.join(input_files_path, file))
        elif file.split('.')[-1] == 'csv':
            import_df = pd.read_csv(os.path.join(input_files_path, file), sep=';')
    else:
        if file.split('.')[-1] == 'xlsx':
            if file.startswith(files_names_start_list[2]):
                import_df = open_excel(os.path.join(input_files_path, file), skiprows=3, dtypes=dtypes)
            elif file.startswith(files_names_start_list[3]):
                import_df = open_excel(os.path.join(input_files_path, file), skiprows=2, dtypes=dtypes)
                if '# de venta' not in import_df.columns:
                    import_df = open_excel(os.path.join(input_files_path, file), skiprows=3, dtypes=dtypes)
            else:
                import_df = open_excel(os.path.join(input_files_path, file), dtypes=dtypes)
        elif file.split('.')[-1] == 'csv':
            import_df = pd.read_csv(os.path.join(input_files_path, file), sep=';', dtype=dtypes)
    return import_df


def remove_duplicates(df, sort_by, rm_cols=None, subset=None):
    df.sort_values(by=sort_by, inplace=True)
    if rm_cols is not None:
        cols_to_check = df.columns.tolist()
        for col in rm_cols:
            cols_to_check.remove(col)
        df.drop_duplicates(subset=cols_to_check, keep='last', inplace=True, ignore_index=True)
    elif subset is not None:
        df.drop_duplicates(subset=subset, keep='last', inplace=True, ignore_index=True)
    return df


def main():
    logger.info('Start data processing program')
    data_folder = 'BI'
    main_data_file = 'main_data.xlsx'
    consolidated_file = 'consolidated_data.xlsx'
    cancelled_file = 'cancelled_sales.xlsx'
    inventory_file = 'total_inventory.xlsx'
    archive_data = 'Archive'
    working_path = os.getcwd()
    input_files_path = os.path.join(working_path, data_folder)
    archive_path = os.path.join(input_files_path, archive_data)
    historical_path = os.path.join(working_path, main_data_file)
    consolidated_path = os.path.join(working_path, consolidated_file)
    cancelled_path = os.path.join(working_path, cancelled_file)
    inventory_path = os.path.join(working_path, inventory_file)
    c_activities = 0
    c_settle = 0
    c_sales = 0
    days_of_sales = 30
    order_lead_time = 20
    activities = False
    ventas = False
    settlement = False
    stock_casa = False
    stock_full = False
    cost = False
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

    files_names_start = {'activities-collection': 'csv',
                         'settlement-report': 'xlsx',
                         'Stock_general_Full': 'xlsx',
                         'Ventas_CO': 'xlsx',
                         'Inventario MELI (CASA)': 'xlsx',
                         'Tequi_Product_Costs_New': 'xlsx'}

    # Getting the files in the input file directory
    files_in_path = [f for f in os.listdir(input_files_path) if os.path.isfile(os.path.join(input_files_path, f))]
    print(f'files in the path: {files_in_path}')
    # Getting the files to load from the files in the input file path
    files_to_load = [f for f in files_in_path for n in files_names_start.keys()
                     if f.startswith(n) and (fnmatch(f, f'*.{files_names_start[n]}'))]
    print(f'files_to_load: {files_to_load}')

    logger.debug(f'Found {len(files_to_load)} files to process: {files_to_load}')
    if len(files_to_load) > 0:
        files_names_start_list = list(files_names_start.keys())
        main_dtypes = {'item_id': str,
                       'external_reference': str,
                       'operation_id': str,
                       'order_id': str,
                       'pack_id': str
                       }
        if os.path.isfile(historical_path):
            logger.debug('Opening historical data')
            historical_df = open_excel(historical_path, dtypes=main_dtypes)
        else:
            historical_df = pd.DataFrame(columns=['date_created', 'item_id', 'reason', 'external_reference', 'SKU',
                                                  'operation_id', 'status', 'status_detail', 'operation_type',
                                                  'transaction_amount', 'sale_amount', 'marketplace_fee',
                                                  'shipping_cost_by_seller', 'shipping_cost_by_customer',
                                                  'coupon_fee', 'taxes_amount', 'net_received_amount', 'payment_type',
                                                  'amount_refunded', 'order_id', 'shipment_status', 'time_created',
                                                  'file_date', 'quantity', 'marketplace', 'pack_id', 'cost'])
        for file in files_to_load:
            logger.debug(f'Processing {file} file')
            try:
                # Assigning the temp dataframe to the corresponding dataframe considering the filename
                if file.startswith(files_names_start_list[0]):
                    dtypes = {'Identificador de producto (item_id)': str,
                              'Código de referencia (external_reference)': str,
                              'Número de operación de Mercado Pago (operation_id)': str,
                              'Número de venta en Mercado Libre (order_id)': str
                              }
                    file_date = datetime.strptime(re.findall(r'-([0-9]{14})-', file)[0], '%Y%m%d%H%M%S')
                    date_str = file_date.strftime('%Y%m%d')
                    temp = import_file(file, files_names_start_list, input_files_path, dtypes)
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
                    dtypes = {'SOURCE_ID': str,
                              'EXTERNAL_REFERENCE': str,
                              'ORDER_ID': str,
                              'PACK_ID': str
                              }
                    file_date = datetime.strptime(''.join(re.findall(r'-([0-9]{4})-([0-9]{2})-([0-9]{1,2})', file)[0]),
                                                  '%Y%m%d')
                    date_str = file_date.strftime('%Y%m%d')
                    temp = import_file(file, files_names_start_list, input_files_path, dtypes)
                    temp['file_date'] = file_date.date()
                    if c_settle == 0:
                        settlement_report = temp
                    else:
                        settlement_report = pd.concat([settlement_report, temp], axis=0)
                    c_settle += 1
                    settlement = True
                    archive = True
                elif file.startswith(files_names_start_list[2]):
                    dtypes = {'ID de publicación': str}
                    date_str = datetime.strptime(''.join(re.findall(r'_([0-9]{1,2})-([0-9]{2})-([0-9]{4})_', file)[0]),
                                             '%d%m%Y').strftime('%Y%m%d')
                    temp = import_file(file, files_names_start_list, input_files_path, dtypes)
                    temp.rename(columns={'Código ML': 'ml_code', 'ID de publicación': 'MCO'}, inplace=True)
                    stock_general_full = temp
                    stock_full = True
                    archive = True
                elif file.startswith(files_names_start_list[3]):
                    dtypes = {'# de venta': str,
                              '# de publicación': str}
                    date_str = re.findall(r'_([0-9]{1,2})_de_([a-z]{3,10})_de_([0-9]{4})', file)[0]
                    date_str = datetime.strptime(date_str[2]+month_dict[date_str[1]]+date_str[0], '%Y%m%d').strftime('%Y%m%d')
                    temp = import_file(file, files_names_start_list, input_files_path, dtypes)
                    cols = ['# de venta', 'Fecha de venta', 'Estado', 'Unidades', 'Ingresos por productos (COP)',
                            'Ingresos por envío (COP)', 'Cargo por venta e impuestos', 'Costos de envío',
                            'Anulaciones y reembolsos (COP)', 'Total (COP)', 'SKU',
                            '# de publicación', 'Canal de venta', 'Título de la publicación', 'Variante',
                            'Precio unitario de venta de la publicación (COP)', 'Tipo de publicación']
                    temp = temp[cols]
                    if c_sales == 0:
                        ventas_co = temp
                    else:
                        ventas_co = pd.concat([ventas_co, temp], axis=0)
                    c_sales += 1
                    ventas = True
                    archive = True
                elif file.startswith(files_names_start_list[4]):
                    dtypes = {'CÓD ML / SKU': str,
                              '# Publicacion': str}
                    temp = import_file(file, files_names_start_list, input_files_path, dtypes)
                    temp = temp[['CÓD ML / SKU', '# Publicacion', 'Provider', 'Title', 'Referencia',
                                 'Detalle', 'Estado', 'Inventario CASA']]
                    temp.rename(columns={'CÓD ML / SKU': 'SKU'}, inplace=True)
                    stock_casa_df = temp
                    stock_casa = True
                    archive = False
                elif file.startswith(files_names_start_list[5]):
                    dtypes = {'# Publicacion': str}
                    temp = import_file(file, files_names_start_list, input_files_path, dtypes)
                    cols = ['# Publicacion', 'Total costo COP']
                    temp = temp[cols]
                    cost_df = temp
                    cost = True
                    archive = False

                # Moving the current file to an archive except for the house inventory file
                if archive:
                    logger.debug(f'Moving the file {file} to the archive')
                    do_archive(input_files_path, archive_path, date_str, file)

            except Exception as ex:
                logger.error(ex)
                logger.error(traceback.format_exc())

        try:
            logger.debug(f'There are {len(activities_collection)} records to be added')
            if len(activities_collection) > 0:
                if activities and settlement and ventas and cost:
                    logger.debug('Removing duplicates from the main files')
                    activities_collection = remove_duplicates(activities_collection,
                                                              sort_by=['date_created', 'file_date'],
                                                              rm_cols=['file_date'])
                    settlement_report = remove_duplicates(settlement_report,
                                                          sort_by=['ORIGIN_DATE', 'file_date'],
                                                          rm_cols=['file_date'])
                    ventas_co = remove_duplicates(ventas_co,
                                                  sort_by=['# de venta'],
                                                  subset=['# de venta'])
                    logger.debug('Populating the missing marketplace fees')
                    activities_collection, refunded_sales = populate_missing_fields(activities_collection, settlement_report)
                    logger.debug('Adding Shipping cost by customer')
                    activities_collection = add_shipping_cost_by_customer(activities_collection)
                    logger.debug('Recalculating Net received amount')
                    activities_collection = calculate_net_received_amount(activities_collection)
                    logger.debug('Removing Cancelled sales')
                    activities_collection = remove_cancelled_sales(activities_collection, cancelled_path, main_dtypes)
                    logger.debug('concatenating main and refunded data')
                    activities_collection = add_refunded_sales(activities_collection, refunded_sales)
                    logger.debug('Adding the marketplace and the quantities sold for each product')
                    activities_collection = add_quantities_marketplace(activities_collection, ventas_co)
                    logger.debug('Fixing the refunded values')
                    activities_collection = fix_refunded_sales(activities_collection)
                    logger.debug('Grouping and aggregating the main data')
                    activities_collection = data_aggregation(activities_collection)
                    activities_collection['item_id'] = activities_collection['item_id'].apply(lambda x: str(x).strip('MCO'))
                    logger.debug('Adding the cost of the products')
                    activities_collection = add_product_cost(activities_collection, cost_df)
                    logger.debug('Generating Auxiliary File')
                    aux_data = generate_aux_data(activities_collection)

                    # Re-ordering de columns before concatenating it with the historical data
                    activities_collection = activities_collection[historical_df.columns.tolist()]
                    # Assigning the dtypes from activities_collection to the historical df
                    historical_df = historical_df.astype(activities_collection.dtypes)
                    # Inserting new sales into the historical data files
                    main_data = pd.concat([historical_df, activities_collection], axis=0).reset_index(drop=True)
                    if os.path.isfile(consolidated_path):
                        historical_consolidated = open_excel(consolidated_path, dtypes=main_dtypes)
                    else:
                        historical_consolidated = pd.DataFrame(columns=['date_created', 'item_id', 'reason',
                                                                        'external_reference', 'SKU', 'operation_id',
                                                                        'status', 'status_detail', 'operation_type',
                                                                        'amount', 'payment_type', 'order_id',
                                                                        'shipment_status', 'time_created', 'file_date',
                                                                        'quantity', 'transaction_type', 'marketplace',
                                                                        'pack_id'
                                                                        ])
                    aux_data = aux_data[historical_consolidated.columns.tolist()]
                    historical_consolidated = historical_consolidated.astype(aux_data.dtypes)
                    consolidated_data = pd.concat([historical_consolidated, aux_data], axis=0).reset_index(drop=True)
                    # Saving the files with the new data added
                    logger.debug('Saving sales files...')
                    main_data.to_excel(historical_path, index=False, sheet_name='main')
                    consolidated_data.to_excel('consolidated_data.xlsx', index=False, sheet_name='consolidated')
                    logger.debug('Saving sales files process finished')
                else:
                    logger.info('Some of the sales data are missing in the input files path')
            else:
                logger.info('There is no new data to add')

            if stock_casa and stock_full:
                logger.debug('Processing inventory files')
                inventory = stock_casa_df.merge(how='left',
                                                right=stock_general_full.loc[:, ['ml_code', 'Stock total almacenado']],
                                                left_on=['SKU'],
                                                right_on=['ml_code']
                                                )
                inventory['Stock total almacenado'].fillna(value=0, inplace=True)
                inventory['Total'] = inventory['Stock total almacenado'] + inventory['Inventario CASA']
                inventory.drop(columns=['ml_code'], inplace=True)

                # Get the sales from the historic file
                sales_hist = open_excel(historical_path, dtypes=main_dtypes)
                sales_hist.sort_values(by='date_created', ascending=False, inplace=True)
                # Merging the historic sales with the last sales dates
                last_sales_df = sales_hist
                last_sales_df = last_sales_df.merge(how='left', right=last_sales_df.loc[
                    ~last_sales_df.duplicated(subset=['SKU']), ['SKU', 'date_created']], on='SKU')
                last_sales_df.rename(columns={'date_created_x': 'date_created', 'date_created_y': 'date_last_sale'},
                                     inplace=True)
                last_sales_df['start_date_range'] = last_sales_df['date_last_sale'] - timedelta(days_of_sales)
                sales_range_df = last_sales_df.loc[(last_sales_df['start_date_range'] <= last_sales_df['date_created'])
                                                   & (last_sales_df['date_last_sale'] >= last_sales_df['date_created']),
                                 :]
                sold_units = sales_range_df.groupby(['SKU'])['quantity'].sum().reset_index()
                inventory = inventory.merge(how='left', right=sold_units, left_on='SKU', right_on='SKU')
                inventory.rename(columns={'quantity': 'units_sold'}, inplace=True)
                inventory['units_sold'].fillna(0, inplace=True)
                inventory = inventory.merge(how='left', right=sales_range_df.loc[~sales_range_df.duplicated(
                    subset=['SKU']), ['SKU', 'date_last_sale', 'start_date_range']], left_on='SKU', right_on='SKU')
                logger.debug('Adding additional variables to the inventory table')
                # Adding additional variables to the inventory table
                inventory['daily_avg'] = inventory['units_sold']/30
                inventory.loc[inventory['daily_avg'] != 0,
                              'days_of_inv'] = inventory.loc[inventory['daily_avg'] != 0, 'Total']/\
                                               inventory.loc[inventory['daily_avg'] != 0, 'daily_avg']
                inventory.loc[(inventory['daily_avg'] == 0) & (inventory['Total'] == 0), 'days_of_inv'] = 0
                inventory.loc[(inventory['daily_avg'] == 0) & (inventory['Total'] != 0), 'days_of_inv'] = 365
                inventory['days_of_inv'] = inventory['days_of_inv'].apply(lambda x: x if x <= 365 else 365)
                inventory['60_days_inv'] = inventory['daily_avg'] * 60.0
                inventory['sales_until_arrival'] = inventory['daily_avg'] * order_lead_time
                inventory['units_avl_lt'] = inventory['Total'] - inventory['sales_until_arrival']
                inventory.loc[inventory['units_avl_lt'] > 0,
                              'suggested_order'] = inventory.loc[inventory['units_avl_lt'] > 0, '60_days_inv']\
                                                   - inventory.loc[inventory['units_avl_lt'] > 0,'units_avl_lt']
                inventory.loc[inventory['units_avl_lt'] <= 0,
                              'suggested_order'] = inventory.loc[inventory['units_avl_lt'] <= 0, '60_days_inv']
                inventory['suggested_order'] = inventory['suggested_order'].apply(lambda x: x if x > 0 else 0)
                inventory['inventory_time_group'] = inventory.apply(lambda x: 'agotado' if x.days_of_inv == 0
                                                  else ('0 - 7 días' if x.days_of_inv <= 7
                                                        else ('7 - 15 días' if (x.days_of_inv > 7)
                                                                               and (x.days_of_inv <= 15)
                                                              else ('15 - 30 días' if (x.days_of_inv > 15)
                                                                                   and (x.days_of_inv <= 30)
                                                                    else( '1 - 2 meses' if (x.days_of_inv > 30)
                                                                                        and (x.days_of_inv <= 60)
                                                                          else('2 - 3 meses' if (x.days_of_inv > 60)
                                                                                             and (x.days_of_inv <= 90)
                                                                               else('3 - 6 meses' if (x.days_of_inv
                                                                                                      > 90)
                                                                                                     and (x.days_of_inv
                                                                                                          <= 180)
                                                                                    else '> 6 meses'
                                                                               )
                                                                          )
                                                                    )
                                                              )
                                                        )
                                                  )
                , axis=1)
                logger.debug('Saving inventory file...')
                inventory.to_excel(inventory_path, index=False, sheet_name='inventory')
                logger.debug('Saving inventory file process finished')
            else:
                logger.info('Some of the inventory files is missing')

        except Exception as ex:
            logger.error(ex)
            logger.error(traceback.format_exc())
    else:
        logger.debug('There are no files to process')
    logger.info('Data processing is done')
    logger.info('')


if __name__ == '__main__':
    main()
