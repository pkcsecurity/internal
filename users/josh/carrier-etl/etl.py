import os

import bonobo
import sqlalchemy
from sqlalchemy import Table, MetaData
from sqlalchemy.dialects.postgresql import insert
import fedex

# This isn't really the right way to configure a DB connection with bonobo, it should be in get_services
import ups

engine = sqlalchemy.create_engine(os.environ['DATABASE_URL'])


def filter_table(table_name):
    def value_generator(table, row):
        print(table, row)
        if table == table_name:
            yield row

    return value_generator


def get_services(**options):
    return {}


def insert_into(tablename, constraint):
    def table_insert(row):
        upsert = insert(Table(tablename, MetaData(), autoload=True, autoload_with=engine)) \
            .on_conflict_do_update(constraint=constraint, set_=row)
        engine.connect().execute(upsert, row)

    return table_insert


def get_graph(**options):
    graph = bonobo.Graph()
    graph.add_chain(
        fedex.extract,
        fedex.transform,
        filter_table('shipments'),
        insert_into('shipments', 'shipments_pkey'),
    )
    graph.add_chain(
        filter_table('charges'),
        insert_into('charges', 'charges_charge_index_shipment_id_key'),
        _input=fedex.transform
    )
    return graph


if __name__ == '__main__':
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(
            get_graph(**options),
            services=get_services(**options)
        )
