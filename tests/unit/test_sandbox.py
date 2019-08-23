import json

import psycopg2
import psycopg2.extras
import pytest

from utils.fixtures import CONFIG, db_cleanup, ListStream, TEST_DB
from target_postgres import main


def assert_tables_equal(cursor, expected_table_names):
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    tables = []
    for table in cursor.fetchall():
        tables.append(table[0])

    assert (not tables and not expected_table_names) \
           or set(tables) == expected_table_names


def assert_columns_equal(cursor, table_name, expected_column_tuples):
    cursor.execute("SELECT column_name, data_type, is_nullable FROM information_schema.columns " + \
                   "WHERE table_schema = 'public' and table_name = '{}';".format(
                       table_name))
    columns = cursor.fetchall()

    assert (not columns and not expected_column_tuples) \
           or set(columns) == expected_column_tuples


def assert_count_equal(cursor, table_name, n):
    cursor.execute('SELECT count(*) FROM "public"."{}"'.format(table_name))
    assert cursor.fetchone()[0] == n


class BigCommerceStream(ListStream):
    stream = [
        {"type": "SCHEMA",
         "stream": "products",
         "schema": {
             "type": "object",
             "properties": {"id": {"type": "integer"},
                            "name": {"type": ["null",
                                              "string"]},
                            "type": {"type": ["null",
                                              "string"]},
                            "sku": {"type": ["null",
                                             "string"]},
                            "description": {"type": ["null",
                                                     "string"]},
                            "weight": {"type": ["null",
                                                "integer"]},
                            "width": {"type": ["null",
                                               "integer"]},
                            "depth": {"type": ["null",
                                               "integer"]},
                            "height": {"type": ["null",
                                                "integer"]},
                            "price": {"type": ["null",
                                               "integer",
                                               "number"]},
                            "cost_price": {"type": ["null",
                                                    "integer"]},
                            "retail_price": {"type": ["null",
                                                      "integer"]},
                            "sale_price": {"type": ["null",
                                                    "integer"]},
                            "map_price": {"type": ["null",
                                                   "integer"]},
                            "tax_class_id": {"type": ["null",
                                                      "integer"]},
                            "product_tax_code": {"type": ["null",
                                                          "string"]},
                            "calculated_price": {"type": ["null",
                                                          "integer",
                                                          "number"]},
                            "categories": {"type": ["null",
                                                    "array"],
                                           "items": {"type": ["null",
                                                              "integer"]}},
                            "brand_id": {"type": ["null",
                                                  "integer"]},
                            "option_set_id": {"type": ["null",
                                                       "integer"]},
                            "option_set_display": {"type": ["null",
                                                            "string"]},
                            "inventory_level": {"type": ["null",
                                                         "integer"]},
                            "inventory_warning_level": {"type": ["null",
                                                                 "integer"]},
                            "inventory_tracking": {"type": ["null",
                                                            "string"]},
                            "reviews_rating_sum": {"type": ["null",
                                                            "integer"]},
                            "reviews_count": {"type": ["null",
                                                       "integer"]},
                            "total_sold": {"type": ["null",
                                                    "integer"]},
                            "fixed_cost_shipping_price": {"type": ["null",
                                                                   "integer"]},
                            "is_free_shipping": {"type": ["null",
                                                          "boolean"]},
                            "is_visible": {"type": ["null",
                                                    "boolean"]},
                            "is_featured": {"type": ["null",
                                                     "boolean"]},
                            "related_products": {"type": ["null",
                                                          "array"],
                                                 "items": {"type": ["null",
                                                                    "integer"]}},
                            "warranty": {"type": ["null",
                                                  "string"]},
                            "bin_picking_number": {"type": ["null",
                                                            "string"]},
                            "layout_file": {"type": ["null",
                                                     "string"]},
                            "upc": {"type": ["null",
                                             "string"]},
                            "mpn": {"type": ["null",
                                             "string"]},
                            "gtin": {"type": ["null",
                                              "string"]},
                            "search_keywords": {"type": ["null",
                                                         "string"]},
                            "availability": {"type": ["null",
                                                      "string"]},
                            "availability_description": {"type": ["null",
                                                                  "string"]},
                            "gift_wrapping_options_type": {"type": ["null",
                                                                    "string"]},
                            "sort_order": {"type": ["null",
                                                    "integer"]},
                            "condition": {"type": ["null",
                                                   "string"]},
                            "is_condition_shown": {"type": ["null",
                                                            "boolean"]},
                            "order_quantity_minimum": {"type": ["null",
                                                                "integer"]},
                            "order_quantity_maximum": {"type": ["null",
                                                                "integer"]},
                            "page_title": {"type": ["null",
                                                    "string"]},
                            "meta_description": {"type": ["null",
                                                          "string"]},
                            "date_created": {"type": "string",
                                             "format": "date-time"},
                            "date_modified": {"type": "string",
                                              "format": "date-time"},
                            "view_count": {"type": ["null",
                                                    "integer"]},
                            "preorder_release_date": {"type": ["null",
                                                               "string"],
                                                      "format": "date-time"},
                            "preorder_message": {"type": ["null",
                                                          "string"]},
                            "is_preorder_only": {"type": ["null",
                                                          "boolean"]},
                            "is_price_hidden": {"type": ["null",
                                                         "boolean"]},
                            "price_hidden_label": {"type": ["null",
                                                            "string"]},
                            "custom_url": {
                                "type": ["null",
                                         "object"],
                                "properties": {"url": {"type": ["null",
                                                                "string"]},
                                               "is_customized": {"type": ["null",
                                                                          "boolean"]}}},
                            "base_variant_id": {"type": ["null",
                                                         "integer"]},
                            "open_graph_type": {"type": ["null",
                                                         "string"]},
                            "open_graph_title": {"type": ["null",
                                                          "string"]},
                            "open_graph_description": {"type": ["null",
                                                                "string"]},
                            "open_graph_use_meta_description": {"type": ["null",
                                                                         "boolean"]},
                            "open_graph_use_product_name": {"type": ["null",
                                                                     "boolean"]},
                            "open_graph_use_image": {"type": ["null",
                                                              "boolean"]}}},
         "key_properties": ["id"]},
        {"type": "RECORD",
         "stream": "products",
         "record": {"id": 1,
                    "name": "SAMPLE",
                    "type": "physical",
                    "sku": "very-sku-y",
                    "description": "<p>some</p>\n<p>random</p>\n<p>html</p>",
                    "weight": 123,
                    "width": 0,
                    "depth": 0,
                    "height": 0,
                    "price": 31.45,
                    "cost_price": 0,
                    "retail_price": 0,
                    "sale_price": 0,
                    "map_price": 0,
                    "tax_class_id": 0,
                    "product_tax_code": "",
                    "calculated_price": 31.45,
                    "categories": [32, 22, 21, 20],
                    "brand_id": 42,
                    "option_set_id": None,
                    "option_set_display": "right",
                    "inventory_level": 0,
                    "inventory_warning_level": 0,
                    "inventory_tracking": "none",
                    "reviews_rating_sum": 0,
                    "reviews_count": 0,
                    "total_sold": 0,
                    "fixed_cost_shipping_price": 0,
                    "is_free_shipping": False,
                    "is_visible": True,
                    "is_featured": False,
                    "related_products": [-1],
                    "warranty": "",
                    "bin_picking_number": "0",
                    "layout_file": "a-product.html",
                    "upc": "",
                    "mpn": "",
                    "gtin": "",
                    "search_keywords": "",
                    "availability": "available",
                    "availability_description": "",
                    "gift_wrapping_options_type": "any",
                    "sort_order": 0,
                    "condition": "New",
                    "is_condition_shown": False,
                    "order_quantity_minimum": 0,
                    "order_quantity_maximum": 0,
                    "page_title": "",
                    "meta_description": "",
                    "date_created": "2018-08-27T18:40:23.000000Z",
                    "date_modified": "2018-08-27T20:45:53.000000Z",
                    "view_count": 31,
                    "preorder_release_date": None,
                    "preorder_message": "0",
                    "is_preorder_only": False,
                    "is_price_hidden": False,
                    "price_hidden_label": "0",
                    "custom_url": {"url": "/SAMPLE/",
                                   "is_customized": False},
                    "base_variant_id": 77,
                    "open_graph_type": "product",
                    "open_graph_title": "",
                    "open_graph_description": "",
                    "open_graph_use_meta_description": True,
                    "open_graph_use_product_name": True,
                    "open_graph_use_image": True}},
        {"type": "STATE",
         "value": {"bookmarks": {"products": "2018-11-17T21:26:50+00:00"}}},
        {"type": "SCHEMA",
         "stream": "customers",
         "schema": {
             "properties": {"id": {"type": "integer"},
                            "company": {"type": ["null",
                                                 "string"]},
                            "first_name": {"type": ["null",
                                                    "string"]},
                            "last_name": {"type": ["null",
                                                   "string"]},
                            "email": {"type": ["null",
                                               "string"]},
                            "phone": {"type": ["null",
                                               "string"]},
                            "form_fields": {"type": ["null"]},
                            "date_created": {"format": "date-time",
                                             "type": "string"},
                            "date_modified": {"format": "date-time",
                                              "type": "string"},
                            "store_credit": {"type": ["null",
                                                      "string"]},
                            "registration_ip_address": {"type": ["null",
                                                                 "string"]},
                            "customer_group_id": {"type": ["null",
                                                           "integer"]},
                            "notes": {"type": ["null",
                                               "string"]},
                            "tax_exempt_category": {"type": ["null",
                                                             "string"]},
                            "reset_pass_on_login": {"type": ["null",
                                                             "boolean"]},
                            "accepts_marketing": {"type": ["null",
                                                           "boolean"]},
                            "addresses": {
                                "properties": {"url": {"type": ["null",
                                                                "string"]},
                                               "resource": {"type": ["null",
                                                                     "string"]}},
                                "type": ["null",
                                         "object"]}},
             "type": ["null",
                      "object"]},
         "key_properties": ["id"]},
        {"type": "RECORD",
         "stream": "customers",
         "record": {"id": 1,
                    "company": "",
                    "first_name": "Data",
                    "last_name": "Mill",
                    "email": "test@test.com",
                    "phone": "1231231234",
                    "form_fields": None,
                    "date_created": "2018-11-17T21:25:00.000000Z",
                    "date_modified": "2018-11-17T21:25:01.000000Z",
                    "store_credit": "0.0000",
                    "registration_ip_address": "127.0.0.1",
                    "customer_group_id": 0,
                    "notes": "",
                    "tax_exempt_category": "",
                    "reset_pass_on_login": False,
                    "accepts_marketing": False,
                    "addresses": {"url": "https://api.bigcommerce.com/stores/some-unique-hash/v2/customers/1/addresses",
                                  "resource": "/customers/1/addresses"}}},
        {"type": "STATE",
         "value": {"bookmarks": {"products": "2018-11-17T21:26:50+00:00",
                                 "customers": "2018-11-17T21:25:01+00:00"}}}]


def test_bigcommerce__sandbox(db_cleanup):
    main(CONFIG, input_stream=BigCommerceStream())

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_tables_equal(cur,
                                {'products',
                                 'customers',
                                 'products__categories',
                                 'products__related_products'})

            ## form_fields should not show up as it can only be `null`
            assert_columns_equal(cur,
                                 'customers',
                                 {
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('date_modified', 'timestamp with time zone', 'NO'),
                                     ('store_credit', 'text', 'YES'),
                                     ('notes', 'text', 'YES'),
                                     ('tax_exempt_category', 'text', 'YES'),
                                     ('email', 'text', 'YES'),
                                     ('company', 'text', 'YES'),
                                     ('customer_group_id', 'bigint', 'YES'),
                                     ('registration_ip_address', 'text', 'YES'),
                                     ('date_created', 'timestamp with time zone', 'NO'),
                                     ('accepts_marketing', 'boolean', 'YES'),
                                     ('addresses__resource', 'text', 'YES'),
                                     ('reset_pass_on_login', 'boolean', 'YES'),
                                     ('addresses__url', 'text', 'YES'),
                                     ('first_name', 'text', 'YES'),
                                     ('phone', 'text', 'YES'),
                                     ('last_name', 'text', 'YES')
                                 })


class HubspotStream(ListStream):
    stream = [
        {"type": "SCHEMA",
         "stream": "deals",
         "schema": {
             "type": "object",
             "properties": {
                 "properties": {
                     "type": "object",
                     "properties": {
                         "num_contacted_notes": {
                             "type": "object",
                             "properties": {
                                 "value": {
                                     "type": ["null", "number", "string"]
                                 }}}}}}},
         "key_properties": []},
        {"type": "RECORD",
         "stream": "deals",
         "record": {}},
        {"type": "RECORD",
         "stream": "deals",
         "record": {
             "properties": {}}},
        {"type": "RECORD",
         "stream": "deals",
         "record": {
             "properties": {
                 "num_contacted_notes": {}}}},
        {"type": "RECORD",
         "stream": "deals",
         "record": {
             "properties": {
                 "num_contacted_notes": {
                     "value": None}}}},
        {"type": "RECORD",
         "stream": "deals",
         "record": {
             "properties": {
                 "num_contacted_notes": {
                     "value": "helloworld"}}}},
        {"type": "RECORD",
         "stream": "deals",
         "record": {
             "properties": {
                 "num_contacted_notes": {
                     "value": 12345}}}},
        {"type": "RECORD",
         "stream": "deals",
         "record": {
             "properties": {
                 "num_contacted_notes": {
                     "value": 12345.6789}}}}]


def test_hubspot__sandbox(db_cleanup):
    config = CONFIG.copy()
    config['persist_empty_tables'] = True
    main(config, input_stream=HubspotStream())

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_tables_equal(cur,
                                {'deals'})

            assert_columns_equal(cur,
                                 'deals',
                                 {
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_primary_key', 'text', 'NO'),
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('properties__num_contacted_notes__value__f', 'double precision', 'YES'),
                                     ('properties__num_contacted_notes__value__s', 'text', 'YES')
                                 })

            assert_count_equal(cur,
                               'deals',
                               7)
