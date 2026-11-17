from .base import ConnectorRegistry
from .cdc import CdcConnector
from .file import FileConnector
from .file import watermark_lower_bound
from .jdbc import JdbcConnector
from .rest_api import RestApiConnector
from .salesforce_bulk import SalesforceBulkApiConnector
from .streaming import StreamingConnector


def default_connectors() -> ConnectorRegistry:
    registry = ConnectorRegistry()
    for name, connector in {
        "REST_API": RestApiConnector(), "SALESFORCE_BULK_API": SalesforceBulkApiConnector(),
        "JDBC": JdbcConnector(), "FILE": FileConnector(),
        "CDC": CdcConnector(), "STREAMING": StreamingConnector(),
    }.items():
        registry.register(name, connector)
    return registry
