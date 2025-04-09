from viam.components.sensor import Sensor
from viam.resource.registry import Registry, ResourceCreatorRegistration

# Import the model
from .report import StockReportEmail

# Register the model
Registry.register_resource_creator(
    Sensor.API,
    StockReportEmail.MODEL,
    ResourceCreatorRegistration(StockReportEmail.new, StockReportEmail.validate_config)
)