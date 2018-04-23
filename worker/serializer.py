'''
    Support custom Avro serializer for transferring data; this can improve performance.
'''

import avro.io
import avro.schema
import io
import logging

AVSC = '''
{
    "namespace": "SHIELD.INGEST",
    "type": "record",
    "name": "list.avsc",
    "fields": [
        {
            "name": "list",
            "type": [{ "items": "string", "type": "array" }, "null"],
            "default": "[]"
        }
    ]
}
'''

def deserialize(rawbytes):
    '''
        Deserialize given bytes according to the supported Avro schema.

    :param rawbytes: A buffered I/O implementation using an in-memory bytes buffer.
    :returns       : List of ``str`` objects, extracted from the binary stream.
    :rtype         : ``list``
    '''
    decoder = avro.io.BinaryDecoder(io.BytesIO(rawbytes))
    reader  = avro.io.DatumReader(avro.schema.parse(AVSC))

    try: return reader.read(decoder)[list.__name__]
    except Exception as exc:
        logging.getLogger('SHIELD.WORKER.SERIALIZER').error('[{0}] {1}'
            .format(exc.__class__.__name__, exc.message))

    return []

def serialize(value):
    '''
        Convert a ``list`` object to an avro-encoded format.

    :param value: List of ``str`` objects.
    :returns    : A buffered I/O implementation using an in-memory bytes buffer.
    :rtype      : ``str``
    '''
    writer   = avro.io.DatumWriter(avro.schema.parse(AVSC))
    rawbytes = io.BytesIO()

    try:
        writer.write({ list.__name__: value }, avro.io.BinaryEncoder(rawbytes))
        return rawbytes
    except avro.io.AvroTypeException:
        logging.getLogger('SHIELD.DC.SERIALIZER').error('The type of ``{0}`` is not '
            'supported by the Avro schema.'.format(type(value).__name__))

    return None
