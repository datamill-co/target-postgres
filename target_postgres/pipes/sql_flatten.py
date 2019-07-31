from target_postgres import denest


def _batch_to_table_batch(batch):
    return {'type': '__DataMill__TABLE_BATCH',
            'stream': batch['stream'],
            'max_version': batch['max_version'],
            'batches': denest.to_table_batches(batch['schema'],
                                               batch['key_properties'],
                                               batch['records'])}


def flatten(iterable):
    for line_data in iterable:
        if line_data['type'] == '__DataMill__BATCH':
            yield _batch_to_table_batch(line_data)
        else:
            yield line_data
