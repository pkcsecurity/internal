import bonobo
import bonobo_sqlalchemy
import sqlalchemy
from sqlalchemy import Table, MetaData
import os

import carrier_formats
import csv
import re

# This isn't really the right way to configure a DB connection with bonobo, it should be in get_services
engine = sqlalchemy.create_engine(os.environ['DATABASE_URL'])
conn = engine.connect()


def extract_fedex():
    with open('fedex.csv') as file:
        lines = csv.reader(file)
        header = lines.__next__()
        for line in lines:
            # This is to deal with the fact that fedex has multiple
            line_dict = {}
            for field_name, content in zip(header, line):
                if field_name in carrier_formats.fedex_multi_fields:
                    if field_name not in line_dict:
                        line_dict[field_name] = []
                    line_dict[field_name].append(content)
                else:
                    line_dict[field_name] = content
            yield line_dict


def permissive_numeric_parse(s):
    try:
        return float(re.sub('[^0-9.-]', '', s))
    except:
        return None


def transform_fedex(args):
    tracking_id = args.get('Express or Ground Tracking ID')
    shipment_date = args.get('Shipment Date')
    shipment_id = tracking_id + '~' + shipment_date
    charge_index = 0
    for description, amount in zip(
            args.get('Tracking ID Charge Description'),
            args.get('Tracking ID Charge Amount')
    ):
        if description and amount:
            charge_index += 1
            yield ('charges',
                   # tracking the index provides a key of uniqueness per-shipment
                   {'charge_index': charge_index,
                    'description': description,
                    'shipment_id': shipment_id,
                    'amount': permissive_numeric_parse(amount)})
    shipment_details = {
        'shipment_id': shipment_id,
        'carrier': 'FedEx',
        'bill_to_account_number': args.get('Bill to Account Number'),
        'invoice_date': args.get('Invoice Date'),
        'invoice_number': args.get('Invoice Number'),
        'payor': args.get('Payor'),
        'tracking_number': tracking_id,
        'transportation_charge_amount': args.get('Transportation Charge Amount'),
        'net_charge_amount': permissive_numeric_parse(args.get('Net Charge Amount')),
        'service_type': args.get('Service Type'),
        'ground_service': args.get('Ground Service'),
        'shipment_date': shipment_date,
        'pod_delivery_date': args.get('POD Delivery Date'),
        'pod_delivery_time': args.get('POD Delivery Time'),
        'actual_weight_amount': permissive_numeric_parse(args.get('Actual Weight Amount')),
        'actual_weight_units': args.get('Actual Weight Units'),
        'rated_weight_amount': permissive_numeric_parse(args.get('Rated Weight Amount')),
        'rated_weight_units': args.get('Rated Weight Units'),
        'number_of_pieces': args.get('Number of Pieces'),
        'bundle_number': args.get('Bundle Number'),
        'meter_number': args.get('Meter Number'),
        'service_packaging': args.get('Service Packaging'),
        'dim_length': permissive_numeric_parse('Dim Length'),
        'dim_width': permissive_numeric_parse('Dim Width'),
        'dim_height': permissive_numeric_parse('Dim Height'),
        'dim_divisor': permissive_numeric_parse('Dim Divisor'),
        'dim_unit': args.get('Dim Unit'),
        'recipient_name': args.get('Recipient Name'),
        'recipient_company': args.get('Recipient Company'),
        'recipient_address_line_1': args.get('Recipient Address Line 1'),
        'recipient_address_line_2': args.get('Recipient Address Line 2'),
        'recipient_city': args.get('Recipient City'),
        'recipient_state': args.get('Recipient State'),
        'recipient_zip_code': args.get('Recipient Zip Code'),
        'recipient_country_territory': args.get('Recipient Country/Territory'),
        'shipper_company': args.get('Shipper Company'),
        'shipper_name': args.get('Shipper Name'),
        'shipper_address_line_1': args.get('Shipper Address Line 1'),
        'shipper_address_line_2': args.get('Shipper Address Line 2'),
        'shipper_city': args.get('Shipper City'),
        'shipper_state': args.get('Shipper State'),
        'shipper_zip_code': args.get('Shipper Zip Code'),
        'shipper_country_territory': args.get('Shipper Country/Territory'),
        'original_customer_reference': args.get('Original Customer Reference'),
        'original_ref_2': args.get('Original Ref#2'),
        'original_ref_3_po_number': args.get('Original Ref#3/PO Number'),
        'zone_code': args.get('Zone Code'),
    }
    yield ('shipments', shipment_details)


def filter_table(table_name):
    def value_generator(table, row):
        print(table, row)
        if table == table_name:
            yield row

    return value_generator


def get_services(**options):
    return {}


def insert_into(tablename):
    def table_insert(row):
        engine.connect().execute(Table(tablename, MetaData(), autoload=True, autoload_with=engine).insert(), row)

    return table_insert


def get_graph(**options):
    graph = bonobo.Graph()
    graph.add_chain(
        extract_fedex,
        transform_fedex,
        filter_table('shipments'),
        insert_into('shipments'),
    )
    graph.add_chain(
        filter_table('charges'),
        insert_into('charges'),
        _input=transform_fedex
    )
    return graph


if __name__ == '__main__':
    """
    from carrier_formats import fedex_cols, ups_cols
    all_cols = [col + '-ups' for col in ups_cols]
    all_cols.extend([col + '-fedex' for col in fedex_cols])
    for col in sorted(all_cols):
        print(col)
    # print("intersection: ", set(ups_cols).intersection(set(fedex_cols)))
    """
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(
            get_graph(**options),
            services=get_services(**options)
        )
    # """
