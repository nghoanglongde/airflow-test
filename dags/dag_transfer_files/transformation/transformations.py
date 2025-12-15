import logging
from datetime import datetime


logger = logging.getLogger(__name__)


def chain_transforms(*transforms):
    def chained_transform(chunk):
        result = chunk
        for transform in transforms:
            result = transform(result)
        return result

    return chained_transform


def uppercase_transform(chunk):
    try:
        text = chunk.decode('utf-8')
        return text.upper().encode('utf-8')
    except UnicodeDecodeError:
        return chunk


def add_timestamp_transform(chunk: bytes) -> bytes:
    timestamp = datetime.utcnow().isoformat()
    prefix = f"[transferred at {timestamp}]\n".encode('utf-8')
    return prefix + chunk


timestamp_and_uppercase_transform = chain_transforms(
    add_timestamp_transform,
    uppercase_transform
)
