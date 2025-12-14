# airflow DAG
from adapters.storage_adapter.sftp_adapter import SFTPStorageAdapter
from typing import Dict, Type


class StorageAdapterFactory:
    _adapters = {
        'sftp': SFTPStorageAdapter,
    }

    @classmethod
    def create_adapter(cls, adapter_type, conn_id):
        adapter_class = cls._adapters.get(adapter_type.lower())
        if not adapter_class:
            supported = ', '.join(cls._adapters.keys())
            raise ValueError(f"unsupported adapter type: {adapter_type}")
        return adapter_class(conn_id)

    @classmethod
    def register_adapter(cls, adapter_type, adapter_class):
        cls._adapters[adapter_type.lower()] = adapter_class
