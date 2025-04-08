import asyncio
from viam.module.module import Module
from viam.components.sensor import Sensor
from .report import WorkbookReportEmail

async def main():
    module = Module.from_args()
    module.add_model_from_registry(Sensor.API, EmailWorkbooks.MODEL)
    await module.start()

if __name__ == "__main__":
    asyncio.run(main())