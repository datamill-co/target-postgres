from smart_open import open
from google.cloud import storage
from datetime import datetime
import os
import simplejson as json

def persist_csv_to_gcs(schema, stream, temp_table_name, columns, config): 
    service_account = storage.Client.from_service_account_info(json.loads(config.get('credentials_json')))
    bucket = config.get('bucket', "datalake_ge93s3dt")
    _256kb = int(256 * 1024)
    date = datetime.now().strftime("%Y-%m-%d")
    with open(f'/tmp/{temp_table_name}.csv', 'r') as f:
        if os.stat(f.name).st_size > 0:
            with open(f"gs://{bucket}/{schema}/{stream}"
            f"/{date}/{temp_table_name}.csv",
            "w",
            transport_params=dict(
                client=service_account,
                buffer_size=_256kb * ((2.5 * 10e6) // _256kb),
                min_part_size = _256kb * ((2.5 * 10e6) // _256kb),
                )) as fh:
                fh.write(", ".join(columns) + "\n")
                fh.write(f.read())
