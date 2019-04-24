import json

import psycopg2
import psycopg2.extras
import pytest

from fixtures import CONFIG, db_cleanup, TEST_DB
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


class SandboxStream:
    idx = None
    stream = NotImplementedError()

    def __init__(self):
        self.idx = -1

    def __iter__(self):
        return self

    def __next__(self):
        self.idx += 1

        if self.idx < len(self.stream):
            return json.dumps(self.stream[self.idx])

        raise StopIteration


class BigCommerceStream(SandboxStream):
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


class HubspotStream(SandboxStream):
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


class TroubleStream(SandboxStream):
    stream = [
        {
            "type": "SCHEMA",
            "stream": "activities",
            "schema": {
                "selected": True,
                "properties": {
                    "envelope": {
                        "properties": {
                            "cc": {
                                "items": {
                                    "properties": {
                                        "email": {
                                            "inclusion": "automatic",
                                            "type": ["null", "string"]
                                        },
                                        "name": {
                                            "inclusion": "automatic",
                                            "type": ["null", "string"]
                                        }
                                    },
                                    "inclusion": "automatic",
                                    "type": ["null", "object"],
                                    "additionalProperties": True
                                },
                                "inclusion": "automatic",
                                "type": ["null", "array"]
                            },
                            "message_id": {
                                "inclusion": "automatic",
                                "type": ["null", "string"]
                            },
                            "sender": {
                                "items": {
                                    "properties": {
                                        "email": {
                                            "inclusion": "automatic",
                                            "type": ["null", "string"]
                                        },
                                        "name": {
                                            "inclusion": "automatic",
                                            "type": ["null", "string"]
                                        }
                                    },
                                    "inclusion": "automatic",
                                    "type": ["null", "object"],
                                    "additionalProperties": True
                                },
                                "inclusion": "automatic",
                                "type": ["null", "array"]
                            },
                            "date": {
                                "inclusion": "automatic",
                                "format": "date-time",
                                "type": [
                                    "null",
                                    "string"
                                ]
                            },
                            "subject": {
                                "inclusion": "automatic",
                                "type": ["null", "string"]
                            },
                            "is_autoreply": {
                                "inclusion": "automatic",
                                "type": ["null", "integer", "boolean"]
                            },
                            "from": {
                                "items": {
                                    "properties": {
                                        "email": {
                                            "inclusion": "automatic",
                                            "type": ["null", "string"]
                                        },
                                        "name": {
                                            "inclusion": "automatic",
                                            "type": ["null", "string"]
                                        }
                                    },
                                    "inclusion": "automatic",
                                    "type": ["null", "object"],
                                    "additionalProperties": True
                                },
                                "inclusion": "automatic",
                                "type": ["null", "array"]
                            },
                            "bcc": {
                                "items": {
                                    "properties": {
                                        "email": {
                                            "inclusion": "automatic",
                                            "type": ["null", "string"]
                                        },
                                        "name": {
                                            "inclusion": "automatic",
                                            "type": ["null", "string"]
                                        }
                                    },
                                    "inclusion": "automatic",
                                    "type": ["null", "object"],
                                    "additionalProperties": True
                                },
                                "inclusion": "automatic",
                                "type": ["null", "array"]
                            },
                            "reply_to": {
                                "items": {
                                    "properties": {
                                        "email": {
                                            "inclusion": "automatic",
                                            "type": ["null", "string"]
                                        },
                                        "name": {
                                            "inclusion": "automatic",
                                            "type": ["null", "string"]
                                        }
                                    },
                                    "inclusion": "automatic",
                                    "type": ["null", "object"],
                                    "additionalProperties": True
                                },
                                "inclusion": "automatic",
                                "type": ["null", "array"]
                            },
                            "to": {
                                "items": {
                                    "properties": {
                                        "email": {
                                            "inclusion": "automatic",
                                            "type": ["null", "string"]
                                        },
                                        "name": {
                                            "inclusion": "automatic",
                                            "type": ["null", "string"]
                                        }
                                    },
                                    "inclusion": "automatic",
                                    "type": ["null", "object"],
                                    "additionalProperties": True
                                },
                                "inclusion": "automatic",
                                "type": ["null", "array"]
                            },
                            "in_reply_to": {
                                "inclusion": "automatic",
                                "type": ["null", "string"]
                            }
                        },
                        "inclusion": "automatic",
                        "type": ["null", "object"],
                        "additionalProperties": True
                    },
                    "date_created": {
                        "inclusion": "automatic",
                        "format": "date-time",
                        "type": ["null", "string"]
                    },
                    "references": {
                        "items": {
                            "inclusion": "automatic",
                            "type": ["null", "string"]
                        },
                        "inclusion": "automatic",
                        "type": ["null", "array"]
                    },
                    "body_text_quoted": {
                        "items": {
                            "properties": {
                                "expand": {
                                    "inclusion": "automatic",
                                    "type": ["null", "integer", "boolean"]
                                },
                                "text": {
                                    "inclusion": "automatic",
                                    "type": ["null", "string"]
                                }
                            },
                            "inclusion": "automatic",
                            "type": ["null", "object"],
                            "additionalProperties": True
                        },
                        "inclusion": "automatic",
                        "type": ["null", "array"]
                    },
                    "need_smtp_credentials": {
                        "inclusion": "automatic",
                        "type": ["null", "integer", "boolean"]
                    },
                    "in_reply_to_id": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "updated_by_name": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "old_status_label": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "transferred_from": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "user_id": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "updated_by": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "cc": {
                        "items": {
                            "inclusion": "automatic",
                            "type": ["null", "string"]
                        },
                        "inclusion": "automatic",
                        "type": ["null", "array"]
                    },
                    "opportunity_value": {
                        "inclusion": "automatic",
                        "type": ["null", "integer"]
                    },
                    "task_assigned_to": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "created_by": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "status": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "email_account_id": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "template_id": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "transferred_to": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "attachments": {
                        "items": {
                            "properties": {
                                "filename": {
                                    "inclusion": "automatic",
                                    "type": ["null", "string"]
                                },
                                "content_id": {
                                    "inclusion": "automatic",
                                    "type": ["null", "string"]
                                },
                                "url": {
                                    "inclusion": "automatic",
                                    "type": ["null", "string"]
                                },
                                "size": {
                                    "inclusion": "automatic",
                                    "type": ["null", "integer"]
                                },
                                "content_type": {
                                    "inclusion": "automatic",
                                    "type": ["null", "string"]
                                }
                            },
                            "inclusion": "automatic",
                            "type": ["null", "object"],
                            "additionalProperties": True
                        },
                        "inclusion": "automatic",
                        "type": ["null", "array"]
                    },
                    "message_ids": {
                        "items": {
                            "inclusion": "automatic",
                            "type": ["null", "string"]
                        },
                        "inclusion": "automatic",
                        "type": ["null", "array"]
                    },
                    "voicemail_duration": {
                        "inclusion": "automatic",
                        "type": ["null", "integer"]
                    },
                    "local_phone": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "body_html_quoted": {
                        "items": {
                            "properties": {
                                "html": {
                                    "inclusion": "automatic",
                                    "type": ["null", "string"]
                                },
                                "expand": {
                                    "inclusion": "automatic",
                                    "type": ["null", "integer", "boolean"]
                                }
                            },
                            "inclusion": "automatic",
                            "type": ["null", "object"],
                            "additionalProperties": True
                        },
                        "inclusion": "automatic",
                        "type": ["null", "array"]
                    },
                    "opens": {
                        "items": {
                            "properties": {
                                "ip_address": {
                                    "inclusion": "automatic",
                                    "type": ["null", "string"]
                                },
                                "user_agent": {
                                    "inclusion": "automatic",
                                    "type": ["null", "string"]
                                },
                                "opened_by": {
                                    "inclusion": "automatic",
                                    "type": ["null", "string"]
                                },
                                "opened_at": {
                                    "inclusion": "automatic",
                                    "type": ["null", "string"]
                                }
                            },
                            "inclusion": "automatic",
                            "type": ["null", "object"],
                            "additionalProperties": True
                        },
                        "inclusion": "automatic",
                        "type": ["null", "array"]
                    },
                    "task_id": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "lead_id": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "task_assigned_to_name": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "body_text": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "thread_id": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "task_text": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "user_name": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "note": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "source": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "import_id": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "to": {
                        "items": {
                            "inclusion": "automatic",
                            "type": ["null", "string"]
                        },
                        "inclusion": "automatic",
                        "type": ["null", "array"]
                    },
                    "recording_url": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "date_scheduled": {
                        "inclusion": "automatic",
                        "format": "date-time",
                        "type": ["null", "string"]
                    },
                    "subject": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "body_preview": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "created_by_name": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "phone": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "sender": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "duration": {
                        "inclusion": "automatic",
                        "type": ["null", "integer"]
                    },
                    "date_sent": {
                        "inclusion": "automatic",
                        "format": "date-time",
                        "type": ["null", "string"]
                    },
                    "_type": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "new_status_label": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "opportunity_value_formatted": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "opportunity_id": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "opens_summary": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "new_status_type": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "remote_phone": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "new_status_id": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "contact_id": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "body_html": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "opportunity_date_won": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "opportunity_value_currency": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "old_status_type": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "bcc": {
                        "items": {
                            "inclusion": "automatic",
                            "type": ["null", "string"]
                        },
                        "inclusion": "automatic",
                        "type": ["null", "array"]
                    },
                    "organization_id": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "old_status_id": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "opportunity_confidence": {
                        "inclusion": "automatic",
                        "type": ["null", "integer"]
                    },
                    "date_updated": {
                        "inclusion": "automatic",
                        "format": "date-time",
                        "type": ["null", "string"]
                    },
                    "template_name": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "opportunity_value_period": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "voicemail_url": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "send_attempts": {
                        "items": {
                            "properties": {
                                "date": {
                                    "inclusion": "automatic",
                                    "format": "date-time",
                                    "type": ["null", "string"]
                                },
                                "error_message": {
                                    "inclusion": "automatic",
                                    "type": ["null", "string"]
                                },
                                "error_class": {
                                    "inclusion": "automatic",
                                    "type": ["null", "string"]
                                }
                            },
                            "inclusion": "automatic",
                            "type": ["null", "object"],
                            "additionalProperties": True
                        },
                        "inclusion": "automatic",
                        "type": ["null", "array"]
                    },
                    "id": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "dialer_id": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "direction": {
                        "inclusion": "automatic",
                        "type": ["null", "string"]
                    },
                    "users": {
                        "items": {
                            "properties": {
                                "first_name": {
                                    "inclusion": "automatic",
                                    "type": ["null", "string"]
                                },
                                "id": {
                                    "inclusion": "automatic",
                                    "type": ["null", "string"]
                                },
                                "organizations": {
                                    "items": {
                                        "inclusion": "automatic",
                                        "type": ["null", "string"]
                                    },
                                    "inclusion": "automatic",
                                    "type": ["null", "array"]
                                },
                                "email": {
                                    "inclusion": "automatic",
                                    "type": ["null", "string"]
                                },
                                "date_updated": {
                                    "inclusion": "automatic",
                                    "format": "date-time",
                                    "type": ["null", "string"]
                                },
                                "last_name": {
                                    "inclusion": "automatic",
                                    "type": ["null", "string"]
                                },
                                "image": {
                                    "inclusion": "automatic",
                                    "type": ["null", "string"]
                                },
                                "date_created": {
                                    "inclusion": "automatic",
                                    "format": "date-time",
                                    "type": ["null", "string"]
                                },
                                "last_used_timezone": {
                                    "inclusion": "automatic",
                                    "type": ["null", "string"]
                                }
                            },
                            "inclusion": "automatic",
                            "type": ["null", "object"],
                            "additionalProperties": True
                        },
                        "inclusion": "automatic",
                        "type": ["null", "array"]
                    }
                },
                "inclusion": "automatic",
                "type": "object",
                "additionalProperties": True
            },
            "key_properties": []
        },
        {
            "type": "RECORD",
            "stream": "activities",
            "record": {
                "body_preview": "Hi Dylan!\n\nYou can send, receive, and track your email conversations all within <a href='https://close.com'>Close</a>.\n\nTry sending an outgoing email - click on the reply arrow and use an Email Templa",
                "need_smtp_credentials": False,
                "in_reply_to_id": None,
                "body_text": "Hi Dylan!\n\nYou can send, receive, and track your email conversations all within <a href='https://close.com'>Close</a>.\n\nTry sending an outgoing email - click on the reply arrow and use an Email Template. You can now check off the Task!\n\nSee the Notes on this Lead and follow the <a href='https://help.close.com/docs/welcome'>Getting Started Guide</a> to get set up as quickly as possible.\n\nLet's make it happen!",
                "date_updated": "2019-04-15T14:38:04.350000Z",
                "created_by_name": "Dylan Driessen",
                "direction": "incoming",
                "contact_id": "cont_KK4hO5MfHtCBklxbwGL4OJPPQ2HZMl1qUXUkeuBxn1Y",
                "thread_id": "acti_jX64VHfU4fMyoIcTa5gxhWwRSvaisTWmxHsZENXCc9u",
                "references": [],
                "message_ids": [],
                "subject": "Welcome to Close, Dylan!",
                "user_id": "user_MOC0Uu4YZ7g5ouIl7UxyxtOLXpHjYkv1853lEdOmq0L",
                "to": ["dylandriessen1@gmail.com"],
                "created_by": "user_MOC0Uu4YZ7g5ouIl7UxyxtOLXpHjYkv1853lEdOmq0L",
                "id": "acti_legxYAqGS0OPC6GUywT19Q4YdUhedERkBdr5waclp3A",
                "cc": [],
                "updated_by_name": "Dylan Driessen",
                "followup_sequence_id": None,
                "template_name": None,
                "user_name": "Dylan Driessen",
                "opens": [],
                "status": "inbox",
                "_type": "Email",
                "updated_by": "user_MOC0Uu4YZ7g5ouIl7UxyxtOLXpHjYkv1853lEdOmq0L",
                "sequence_name": None,
                "body_html_quoted": [
                    {
                        "html": "Hi Dylan!<br><br>You can send, receive, and track your email conversations all within <a href=\"https://close.com\">Close</a>.<br><br>Try sending an outgoing email - click on the reply arrow and use an Email Template. You can now check off the Task!<br><br>See the Notes on this Lead and follow the <a href=\"https://help.close.com/docs/welcome\">Getting Started Guide</a> to get set up as quickly as possible.<br><br>Let's make it happen!",
                        "expand": True
                    }
                ],
                "envelope": {
                    "is_autoreply": False,
                    "from": [{"email": "sales@close.com", "name": "Steli Efti"}],
                    "sender": [{"email": "sales@close.com", "name": "Steli Efti"}],
                    "cc": [],
                    "bcc": [],
                    "to": [{"email": "dylandriessen1@gmail.com", "name": ""}],
                    "date": "2019-04-15T14:38:04.000000Z",
                    "reply_to": [{"email": "sales@close.com", "name": ""}],
                    "in_reply_to": None,
                    "message_id": "<155533908414.190.12671515904402244821@closeio-web-55dff4b9c5-n78pd>",
                    "subject": "Welcome to Close, Dylan!"
                },
                "body_html": "Hi Dylan!<br/><br/>You can send, receive, and track your email conversations all within <a href='https://close.com'>Close</a>.<br/><br/>Try sending an outgoing email - click on the reply arrow and use an Email Template. You can now check off the Task!<br/><br/>See the Notes on this Lead and follow the <a href='https://help.close.com/docs/welcome'>Getting Started Guide</a> to get set up as quickly as possible.<br/><br/>Let's make it happen!",
                "organization_id": "orga_ZEBtiqfqmIVW65FNCr6wHg8cXxZJclBF0634vtxbN6b",
                "sequence_id": None,
                "sequence_subscription_id": None,
                "users": [],
                "body_text_quoted": [
                    {
                        "text": "Hi Dylan!\n\nYou can send, receive, and track your email conversations all within <a href='https://close.com'>Close</a>.\n\nTry sending an outgoing email - click on the reply arrow and use an Email Template. You can now check off the Task!\n\nSee the Notes on this Lead and follow the <a href='https://help.close.com/docs/welcome'>Getting Started Guide</a> to get set up as quickly as possible.\n\nLet's make it happen!",
                        "expand": True
                    }
                ],
                "send_attempts": [],
                "sender": "Steli Efti <sales@close.com>",
                "has_reply": False,
                "lead_id": "lead_qRKMu0ZN45CvnmdlLyvjFLzG1l4H3mf1tkzKjZRBAlh",
                "bcc": [],
                "attachments": [],
                "email_account_id": None,
                "date_sent": None,
                "date_created": "2019-04-15T14:38:04.275000Z",
                "date_scheduled": None,
                "followup_sequence_delay": None,
                "template_id": None,
                "bulk_email_action_id": None,
                "opens_summary": None
            }
        }
    ]


def test_trouble__sandbox(db_cleanup):
    config = CONFIG.copy()
    config['persist_empty_tables'] = True
    main(config, input_stream=TroubleStream())

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_tables_equal(cur,
                                {'activities',
                                 'activities__envelope__cc',
                                 'activities__envelope__sender',
                                 'activities__envelope__from',
                                 'activities__envelope__to',
                                 'activities__envelope__bcc',
                                 'activities__envelope__reply_to',
                                 'activities__body_html_quoted',
                                 'activities__to',
                                 'activities__references',
                                 'activities__opens',
                                 'activities__cc',
                                 'activities__bcc',
                                 'activities__users',
                                 'activities__users__organizations',
                                 'activities__send_attempts',
                                 'activities__attachments',
                                 'activities__body_text_quoted',
                                 'activities__message_ids'})

            assert_columns_equal(cur,
                                 'activities',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_primary_key', 'text', 'NO'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('_type', 'text', 'YES'),
                                     ('body_html', 'text', 'YES'),
                                     ('body_preview', 'text', 'YES'),
                                     ('body_text', 'text', 'YES'),
                                     ('contact_id', 'text', 'YES'),
                                     ('created_by', 'text', 'YES'),
                                     ('created_by_name', 'text', 'YES'),
                                     ('date_created', 'timestamp with time zone', 'YES'),
                                     ('date_scheduled', 'timestamp with time zone', 'YES'),
                                     ('date_sent', 'timestamp with time zone', 'YES'),
                                     ('date_updated', 'timestamp with time zone', 'YES'),
                                     ('dialer_id', 'text', 'YES'),
                                     ('direction', 'text', 'YES'),
                                     ('duration', 'bigint', 'YES'),
                                     ('email_account_id', 'text', 'YES'),
                                     ('envelope__date', 'timestamp with time zone', 'YES'),
                                     ('envelope__in_reply_to', 'text', 'YES'),
                                     ('envelope__is_autoreply__b', 'boolean', 'YES'),
                                     ('envelope__is_autoreply__i', 'bigint', 'YES'),
                                     ('envelope__message_id', 'text', 'YES'),
                                     ('envelope__subject', 'text', 'YES'),
                                     ('id', 'text', 'YES'),
                                     ('import_id', 'text', 'YES'),
                                     ('in_reply_to_id', 'text', 'YES'),
                                     ('lead_id', 'text', 'YES'),
                                     ('local_phone', 'text', 'YES'),
                                     ('need_smtp_credentials__b', 'boolean', 'YES'),
                                     ('need_smtp_credentials__i', 'bigint', 'YES'),
                                     ('new_status_id', 'text', 'YES'),
                                     ('new_status_label', 'text', 'YES'),
                                     ('new_status_type', 'text', 'YES'),
                                     ('note', 'text', 'YES'),
                                     ('old_status_id', 'text', 'YES'),
                                     ('old_status_label', 'text', 'YES'),
                                     ('old_status_type', 'text', 'YES'),
                                     ('opens_summary', 'text', 'YES'),
                                     ('opportunity_confidence', 'bigint', 'YES'),
                                     ('opportunity_date_won', 'text', 'YES'),
                                     ('opportunity_id', 'text', 'YES'),
                                     ('opportunity_value', 'bigint', 'YES'),
                                     ('opportunity_value_currency', 'text', 'YES'),
                                     ('opportunity_value_formatted', 'text', 'YES'),
                                     ('opportunity_value_period', 'text', 'YES'),
                                     ('organization_id', 'text', 'YES'),
                                     ('phone', 'text', 'YES'),
                                     ('recording_url', 'text', 'YES'),
                                     ('remote_phone', 'text', 'YES'),
                                     ('sender', 'text', 'YES'),
                                     ('source', 'text', 'YES'),
                                     ('status', 'text', 'YES'),
                                     ('subject', 'text', 'YES'),
                                     ('task_assigned_to', 'text', 'YES'),
                                     ('task_assigned_to_name', 'text', 'YES'),
                                     ('task_id', 'text', 'YES'),
                                     ('task_text', 'text', 'YES'),
                                     ('template_id', 'text', 'YES'),
                                     ('template_name', 'text', 'YES'),
                                     ('thread_id', 'text', 'YES'),
                                     ('transferred_from', 'text', 'YES'),
                                     ('transferred_to', 'text', 'YES'),
                                     ('updated_by', 'text', 'YES'),
                                     ('updated_by_name', 'text', 'YES'),
                                     ('user_id', 'text', 'YES'),
                                     ('user_name', 'text', 'YES'),
                                     ('voicemail_duration', 'bigint', 'YES'),
                                     ('voicemail_url', 'text', 'YES'),
                                 })

            assert_count_equal(cur,
                               'activities',
                               1)

            cur.execute('SELECT envelope__date FROM "public"."activities"')
            assert cur.fetchone()[0]
