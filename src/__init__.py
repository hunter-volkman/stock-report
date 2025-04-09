from viam.components.sensor import Sensor
from viam.resource.registry import Registry, ResourceCreatorRegistration

# Import the model
from .report import WorkbookReportEmail

# Register the model
Registry.register_resource_creator(
    Sensor.API,
    WorkbookReportEmail.MODEL,
    ResourceCreatorRegistration(WorkbookReportEmail.new, WorkbookReportEmail.validate_config)
)