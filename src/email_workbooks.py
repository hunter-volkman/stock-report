import asyncio
import os
import json
import datetime
from datetime import timedelta
import fasteners
from viam.components.sensor import Sensor
from viam.resource.easy_resource import EasyResource
from viam.resource.types import Model, ModelFamily
from viam.proto.app.robot import ComponentConfig
from viam.proto.common import ResourceName
from viam.utils import SensorReading, struct_to_dict
from viam.logging import getLogger
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from .processor import WorkbookProcessor

LOGGER = getLogger(__name__)

class EmailWorkbooks(Sensor, EasyResource):
    MODEL: Model = Model(ModelFamily("hunter", "sensor"), "worksheet-emailer")

    @classmethod
    def new(cls, config: ComponentConfig, dependencies: dict[ResourceName, "ResourceBase"]) -> "EmailWorkbooks":
        sensor = cls(config)
        sensor.reconfigure(config, dependencies)
        return sensor

    @classmethod
    def validate_config(cls, config: ComponentConfig) -> list[str]:
        attrs = config.attributes.fields
        required = ["email", "password", "recipients", "location", "api_key_id", "api_key", "org_id"]
        for attr in required:
            if attr not in attrs or not attrs[attr].string_value:
                raise ValueError(f"{attr} is required")
        send_time = attrs.get("send_time", "20:00").string_value
        try:
            datetime.datetime.strptime(send_time, "%H:%M")
        except ValueError:
            raise ValueError(f"Invalid send_time '{send_time}': must be in 'HH:MM' format")
        return []

    def __init__(self, config: ComponentConfig):
        super().__init__(config.name)
        self.save_dir = "/home/hunter.volkman/workbooks"
        self.export_script = "/home/hunter.volkman/viam-python-data-export/vde.py"
        self.email = ""
        self.password = ""
        self.recipients = []
        self.send_time = "20:00"
        self.location = ""
        self.api_key_id = ""
        self.api_key = ""
        self.org_id = ""
        self.processor = None
        self.last_processed_date = None
        self.last_sent_time = None
        self.report = "not_sent"
        self.loop_task = None
        self.state_file = os.path.join(self.save_dir, "state.json")
        self.lock_file = os.path.join(self.save_dir, "lockfile")
        self._load_state()

    def _load_state(self):
        if os.path.exists(self.state_file):
            with open(self.state_file, "r") as f:
                state = json.load(f)
                self.last_processed_date = state.get("last_processed_date")
                self.last_sent_time = state.get("last_sent_time")
            LOGGER.info(f"Loaded state: last_processed_date={self.last_processed_date}, last_sent_time={self.last_sent_time}")

    def _save_state(self):
        state = {
            "last_processed_date": self.last_processed_date,
            "last_sent_time": self.last_sent_time
        }
        with open(self.state_file, "w") as f:
            json.dump(state, f)
        LOGGER.info(f"Saved state to {self.state_file}")

    def reconfigure(self, config: ComponentConfig, dependencies: dict[ResourceName, "ResourceBase"]):
        attributes = struct_to_dict(config.attributes)
        self.save_dir = attributes.get("save_dir", "/home/hunter.volkman/workbooks")
        self.export_script = attributes.get("export_script", "/home/hunter.volkman/viam-python-data-export/vde.py")
        self.email = attributes["email"]
        self.password = attributes["password"]
        self.recipients = attributes["recipients"]
        self.send_time = attributes.get("send_time", "20:00")
        self.location = attributes.get("location", "")
        self.api_key_id = attributes["api_key_id"]
        self.api_key = attributes["api_key"]
        self.org_id = attributes["org_id"]

        self.processor = WorkbookProcessor(self.save_dir, self.export_script, self.api_key_id, self.api_key, self.org_id)
        os.makedirs(self.save_dir, exist_ok=True)
        if self.loop_task:
            self.loop_task.cancel()
        self.loop_task = asyncio.create_task(self.run_scheduled_loop())

    def _get_next_send_time(self, now: datetime.datetime) -> datetime.datetime:
        today = now.date()
        send_time_dt = datetime.datetime.combine(today, datetime.datetime.strptime(self.send_time, "%H:%M").time())
        if now > send_time_dt:
            send_time_dt += timedelta(days=1)
        return send_time_dt

    async def run_scheduled_loop(self):
        lock = fasteners.InterProcessLock(self.lock_file)
        if not lock.acquire(blocking=False):
            LOGGER.info(f"Another instance running (PID {os.getpid()}), exiting")
            return
        try:
            LOGGER.info(f"Started scheduled loop with PID {os.getpid()}")
            while True:
                now = datetime.datetime.now()
                today_str = now.strftime("%Y%m%d")
                yesterday_str = (now - timedelta(days=1)).strftime("%Y%m%d")

                next_send = self._get_next_send_time(now)
                sleep_seconds = (next_send - now).total_seconds()
                LOGGER.info(f"Sleeping for {sleep_seconds:.0f} seconds until {next_send}")
                await asyncio.sleep(sleep_seconds)

                now = datetime.datetime.now()
                send_time_today = datetime.datetime.strptime(self.send_time, "%H:%M").time()
                if (now.hour == send_time_today.hour and 
                    now.minute == send_time_today.minute and 
                    self.last_processed_date != yesterday_str):
                    await self.process_and_send(now, yesterday_str)
        except Exception as e:
            LOGGER.error(f"Scheduled loop failed: {e}")
        finally:
            lock.release()

    async def process_and_send(self, timestamp, date_str):
        master_template = os.path.join(self.save_dir, f"3895th_{(timestamp - timedelta(days=1)).strftime('%m%d%y')}.xlsx")
        if not os.path.exists(master_template):
            LOGGER.error(f"Master template {master_template} not found")
            self.report = "error: missing template"
            return

        try:
            workbook_path = self.processor.process(master_template)
            await self.send_workbook(workbook_path, timestamp)
            self.last_processed_date = date_str
            self.last_sent_time = str(timestamp)
            self.report = "sent"
            self._save_state()
            LOGGER.info(f"Processed and sent workbook for {date_str}")
        except Exception as e:
            self.report = f"error: {str(e)}"
            LOGGER.error(f"Failed to process/send for {date_str}: {e}")

    async def send_workbook(self, workbook_path, timestamp):
        msg = MIMEMultipart()
        msg["From"] = self.email
        msg["To"] = ", ".join(self.recipients)
        msg["Subject"] = f"Daily Fill Report - {self.location} - {timestamp.strftime('%Y-%m-%d')}"

        body = f"Attached is the daily langer fill report for {self.location} generated on {timestamp.strftime('%Y-%m-%d')}."
        msg.attach(MIMEText(body, "plain"))

        with open(workbook_path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename={os.path.basename(workbook_path)}",
            )
            msg.attach(part)

        try:
            with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
                smtp.starttls()
                smtp.login(self.email, self.password)
                smtp.send_message(msg)
            LOGGER.info(f"Sent workbook to {msg['To']}")
        except Exception as e:
            LOGGER.error(f"Failed to send email: {e}")
            raise

    async def do_command(self, command: dict, *, timeout: float = None, **kwargs) -> dict:
        if command.get("command") == "process_and_send":
            day = command.get("day", datetime.datetime.now().strftime("%Y%m%d"))
            try:
                timestamp = datetime.datetime.strptime(day, "%Y%m%d")
                await self.process_and_send(timestamp, day)
                return {"status": f"Processed and sent workbook for {day}"}
            except ValueError:
                return {"status": f"Invalid day format: {day}, use YYYYMMDD"}
            except Exception as e:
                return {"status": f"Error: {str(e)}"}
        return {"status": "Unknown command"}

    async def get_readings(self, *, extra: dict = None, timeout: float = None, **kwargs) -> dict[str, SensorReading]:
        now = datetime.datetime.now()
        next_send = self._get_next_send_time(now)
        return {
            "status": "running",
            "last_processed_date": self.last_processed_date or "never",
            "last_sent_time": self.last_sent_time or "never",
            "report": self.report,
            "next_send_time": str(next_send),
            "pid": os.getpid(),
            "location": self.location
        }