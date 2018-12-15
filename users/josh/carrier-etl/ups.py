import csv

from utils import permissive_numeric_parse


def extract():
    with open('ups.csv') as file:
        lines = csv.reader(file)
        header = lines.__next__()
        for line_number, line in enumerate(lines):
            line_dict = {}
            for field_name, content in zip(header, line):
                line_dict[field_name] = content
            yield line_dict, line_number


def format_package_dimensions(args):
    dims = list(map(permissive_numeric_parse, args.get('Package Dimensions').split('x')))
    return tuple(dims) if len(dims) == 3 else (None, None, None)


def format_date(date):
    # This gets the date format into the same as Fedex's
    return date.replace('-', '')


def transform(args, line_number):
    # When we detect fedex's guaranteed delivery date, it will depend on the zone code and service type.  For
    # More detail, see http://images.fedex.com/us/services/pdf/Service_Guide_2017.pdf
    tracking_id = args.get('Tracking Number')
    shipment_date = format_date(args.get('Transaction Date'))
    shipment_id = tracking_id + '~' + shipment_date
    charge_description = args.get('Charge Description')
    charge_amount = permissive_numeric_parse(args.get('Net Amount'))
    if charge_description and charge_amount:
        yield ('charges',
               # tracking the index provides a key of uniqueness per-shipment
               {'charge_index': line_number,
                'description': charge_description,
                'shipment_id': shipment_id,
                'amount': charge_amount})
    length, width, height = format_package_dimensions(args)
    shipment_details = {
        'shipment_id': shipment_id,
        'carrier': 'UPS',
        'bill_to_account_number': args.get('Account Number'),
        'invoice_date': format_date(args.get('Invoice Date')),
        'invoice_number': args.get('Invoice Number'),
        # 'payor': args.get('Payor'),
        'tracking_number': tracking_id,
        # It might make sense to exclude the two fields below, since for all carriers they
        # should instead be represented in the charges table
        # 'transportation_charge_amount': args.get('Transportation Charge Amount'),
        # 'net_charge_amount': permissive_numeric_parse(args.get('Net Charge Amount')),

        # I'm not sure where to get the following two fields right now, but they will be important
        # 'service_type': args.get('Service Type'),
        # 'ground_service': args.get('Ground Service'),

        'shipment_date': shipment_date,
        # Also not sure of the two fields below, but they're important for detecting late shipments
        # (POD means proof-of-delivery and originates from the fedex stuff)
        # 'pod_delivery_date': args.get('POD Delivery Date'),
        # 'pod_delivery_time': args.get('POD Delivery Time'),
        'actual_weight_amount': permissive_numeric_parse(args.get('Entered Weight')),
        'actual_weight_units': args.get('Entered Weight Unit of Measure'),
        'rated_weight_amount': permissive_numeric_parse(args.get('Billed Weight')),
        'rated_weight_units': args.get('Billed Weight Unit of Measure'),
        # For some reason the UPS csvs alternate between 0 and the actual number.  This can be resolved, but
        # will take some care in the upsert step
        # 'number_of_pieces': args.get('Number of Pieces'),
        # 'bundle_number': args.get('Bundle Number'),
        # 'meter_number': args.get('Meter Number'),
        # 'service_packaging': args.get('Service Packaging'),
        'dim_length': length,
        'dim_width': width,
        'dim_height': height,
        # 'dim_divisor': permissive_numeric_parse('Dim Divisor'),
        # 'dim_unit': args.get('Dim Unit'),
        'recipient_name': args.get('Receiver Name'),
        'recipient_company': args.get('Receiver Company Name'),
        'recipient_address_line_1': args.get('Receiver Address Line 1'),
        'recipient_address_line_2': args.get('Receiver Address Line 2'),
        'recipient_city': args.get('Receiver City'),
        'recipient_state': args.get('Receiver State'),
        'recipient_zip_code': args.get('Receiver Postal'),
        'recipient_country_territory': args.get('Receiver Country'),
        'shipper_name': args.get('Sender Name'),
        'shipper_company': args.get('Sender Company Name'),
        'shipper_address_line_1': args.get('Sender Address Line 1'),
        'shipper_address_line_2': args.get('Sender Address Line 2'),
        'shipper_city': args.get('Sender City'),
        'shipper_state': args.get('Sender State'),
        'shipper_zip_code': args.get('Sender Postal'),
        'shipper_country_territory': args.get('Sender Country'),
        'original_customer_reference': args.get('Package Reference Number 1'),
        'original_ref_2': args.get('Package Reference Number 2'),
        'original_ref_3_po_number': args.get('Package Reference Number 3'),
        'zone_code': args.get('Zone'),
    }
    yield ('shipments', shipment_details)
