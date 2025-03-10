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
        attributes = struct_to_dict(config.attributes)
        required = ["email", "password", "recipients", "location", "api_key_id", "api_key", "org_id"]
        for attr in required:
            if attr not in attributes:
                raise Exception(f"{attr} is required")
                
        # Validate send_time
        send_time = attributes.get("send_time", "20:00")
        try:
            datetime.datetime.strptime(str(send_time), "%H:%M")
        except ValueError:
            raise Exception(f"Invalid send_time '{send_time}': must be in 'HH:MM' format")
            
        # Validate process_time if provided
        process_time = attributes.get("process_time")
        if process_time:
            try:
                datetime.datetime.strptime(str(process_time), "%H:%M")
            except ValueError:
                raise Exception(f"Invalid process_time '{process_time}': must be in 'HH:MM' format")
                
        return []

    def __init__(self, config: ComponentConfig):
        super().__init__(config.name)
        self.save_dir = "/home/hunter.volkman/workbooks"
        self.export_script = "/home/hunter.volkman/viam-python-data-export/vde.py"
        self.email = ""
        self.password = ""
        self.recipients = []
        self.send_time = "20:00"
        self.process_time = "19:00"  # Default to 1 hour before send time
        self.location = ""
        self.api_key_id = ""
        self.api_key = ""
        self.org_id = ""
        self.processor = None
        self.last_processed_date = None
        self.last_processed_time = None
        self.last_sent_date = None
        self.last_sent_time = None
        self.data = None
        self.report = "not_sent"
        self.workbook = "not_processed"
        self.loop_task = None
        self.state_file = os.path.join(self.save_dir, "state.json")
        self.lock_file = os.path.join(self.save_dir, "lockfile")
        self._load_state()
        LOGGER.info(f"Initialized EmailWorkbooks with name: {self.name}, save_dir: {self.save_dir}, PID: {os.getpid()}")

    def _load_state(self):
        """Load persistent state from file."""
        if os.path.exists(self.state_file):
            with open(self.state_file, "r") as f:
                state = json.load(f)
                self.last_processed_date = state.get("last_processed_date")
                self.last_processed_time = state.get("last_processed_time")
                self.last_sent_date = state.get("last_sent_date")
                self.last_sent_time = state.get("last_sent_time")
                self.data = state.get("data")
            LOGGER.info(f"Loaded state: last_processed_date={self.last_processed_date}, last_processed_time={self.last_processed_time}, last_sent_date={self.last_sent_date}, last_sent_time={self.last_sent_time}")
        else:
            LOGGER.info(f"No state file at {self.state_file}, starting fresh")

    def _save_state(self):
        """Save state to file for persistence across restarts."""
        state = {
            "last_processed_date": self.last_processed_date,
            "last_processed_time": self.last_processed_time,
            "last_sent_date": self.last_sent_date,
            "last_sent_time": self.last_sent_time,
            "data": self.data
        }
        with open(self.state_file, "w") as f:
            json.dump(state, f)
        LOGGER.info(f"Saved state to {self.state_file}")

    def reconfigure(self, config: ComponentConfig, dependencies: dict[ResourceName, "ResourceBase"]):
        """Configure the module and start the scheduled loop."""
        attributes = struct_to_dict(config.attributes)
        self.save_dir = attributes.get("save_dir", "/home/hunter.volkman/workbooks")
        self.export_script = attributes.get("export_script", "/home/hunter.volkman/viam-python-data-export/vde.py")
        self.email = attributes["email"]
        self.password = attributes["password"]
        
        # Handle recipients properly (could be a string or a list)
        recipients = attributes["recipients"]
        if isinstance(recipients, list):
            self.recipients = recipients
        elif isinstance(recipients, str):
            # Split by comma if it's a string
            self.recipients = [r.strip() for r in recipients.split(",")]
        else:
            LOGGER.warning(f"Unexpected recipients format: {type(recipients)}, using as is")
            self.recipients = [str(recipients)]
            
        self.send_time = attributes.get("send_time", "20:00")
        
        # If process_time is not specified, default to 1 hour before send time
        if "process_time" in attributes:
            self.process_time = attributes["process_time"]
        else:
            # Calculate process_time as 1 hour before send_time
            send_dt = datetime.datetime.strptime(self.send_time, "%H:%M")
            process_dt = send_dt - timedelta(hours=1)
            self.process_time = process_dt.strftime("%H:%M")
            
        self.location = attributes.get("location", "")
        self.api_key_id = attributes["api_key_id"]
        self.api_key = attributes["api_key"]
        self.org_id = attributes["org_id"]

        self.processor = WorkbookProcessor(self.save_dir, self.export_script, self.api_key_id, self.api_key, self.org_id)
        os.makedirs(self.save_dir, exist_ok=True)
        
        LOGGER.info(f"Reconfigured {self.name} with save_dir: {self.save_dir}, recipients: {self.recipients}, "
                   f"location: {self.location}, process_time: {self.process_time}, send_time: {self.send_time}")
        
        if self.loop_task:
            self.loop_task.cancel()
        self.loop_task = asyncio.create_task(self.run_scheduled_loop())

    def _get_next_process_time(self, now: datetime.datetime) -> datetime.datetime:
        """Calculate the next process time based on current time and process_time."""
        today = now.date()
        process_time_dt = datetime.datetime.combine(today, datetime.datetime.strptime(self.process_time, "%H:%M").time())
        if now > process_time_dt:
            process_time_dt += timedelta(days=1)
        return process_time_dt

    def _get_next_send_time(self, now: datetime.datetime) -> datetime.datetime:
        """Calculate the next send time based on current time and send_time."""
        today = now.date()
        send_time_dt = datetime.datetime.combine(today, datetime.datetime.strptime(self.send_time, "%H:%M").time())
        if now > send_time_dt:
            send_time_dt += timedelta(days=1)
        return send_time_dt

    async def run_scheduled_loop(self):
        """Run a scheduled loop that wakes up for processing and sending times."""
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

                next_process = self._get_next_process_time(now)
                next_send = self._get_next_send_time(now)

                # Sleep until the earliest event (process or send)
                sleep_until_process = (next_process - now).total_seconds()
                sleep_until_send = (next_send - now).total_seconds()
                sleep_seconds = min(sleep_until_process, sleep_until_send)
                
                next_event = "process" if sleep_until_process < sleep_until_send else "send"
                LOGGER.info(f"Sleeping for {sleep_seconds:.0f} seconds until {next_event} at "
                          f"{next_process if next_event == 'process' else next_send}")
                
                await asyncio.sleep(sleep_seconds)

                # Check what we woke up for
                now = datetime.datetime.now()
                today_str = now.strftime("%Y%m%d")
                
                # Check if it's time to process
                process_time_today = datetime.datetime.strptime(self.process_time, "%H:%M").time()
                if (now.hour == process_time_today.hour and 
                    now.minute == process_time_today.minute and 
                    self.last_processed_date != today_str):
                    await self.process_workbook(now, today_str)
                
                # Check if it's time to send
                send_time_today = datetime.datetime.strptime(self.send_time, "%H:%M").time()
                if (now.hour == send_time_today.hour and 
                    now.minute == send_time_today.minute and 
                    self.last_sent_date != today_str):
                    await self.send_processed_workbook(now, today_str)
                
        except Exception as e:
            LOGGER.error(f"Scheduled loop failed: {e}")
        finally:
            lock.release()
            LOGGER.info(f"Released lock, loop exiting (PID {os.getpid()})")

    async def process_workbook(self, timestamp, date_str):
        """Process the workbook for the data from yesterday."""
        yesterday = timestamp - timedelta(days=1)
        yesterday_str = yesterday.strftime("%m%d%y")
        master_template = os.path.join(self.save_dir, f"3895th_{yesterday_str}.xlsx")
        
        if not os.path.exists(master_template):
            LOGGER.error(f"Master template {master_template} not found")
            self.workbook = "error: missing template"
            return

        try:
            LOGGER.info(f"Processing workbook using template {master_template}")
            workbook_path = self.processor.process(master_template)
            self.data = workbook_path
            self.last_processed_date = date_str
            self.last_processed_time = str(timestamp)
            self.workbook = "processed"
            self._save_state()
            LOGGER.info(f"Successfully processed workbook for {date_str}, saved at {workbook_path}")
            return workbook_path
        except Exception as e:
            self.workbook = f"error: {str(e)}"
            LOGGER.error(f"Failed to process workbook for {date_str}: {e}")
            return None

    async def send_processed_workbook(self, timestamp, date_str):
        """Send the previously processed workbook."""
        if not self.data or not os.path.exists(self.data):
            LOGGER.error("No processed workbook available to send")
            self.report = "error: no processed workbook"
            return
            
        try:
            await self.send_workbook(self.data, timestamp)
            self.last_sent_date = date_str
            self.last_sent_time = str(timestamp)
            self.report = "sent"
            self._save_state()
            LOGGER.info(f"Sent processed workbook for {date_str}")
        except Exception as e:
            self.report = f"error: {str(e)}"
            LOGGER.error(f"Failed to send workbook for {date_str}: {e}")

    async def process_and_send(self, timestamp, date_str):
        """Process yesterday's data and send the daily workbook immediately."""
        yesterday = timestamp - timedelta(days=1)
        yesterday_str = yesterday.strftime("%m%d%y") 
        master_template = os.path.join(self.save_dir, f"3895th_{yesterday_str}.xlsx")
        
        if not os.path.exists(master_template):
            LOGGER.error(f"Master template {master_template} not found")
            self.report = "error: missing template"
            return

        try:
            workbook_path = await self.process_workbook(timestamp, date_str)
            if workbook_path:
                await self.send_workbook(workbook_path, timestamp)
                self.last_sent_date = date_str
                self.last_sent_time = str(timestamp)
                self.report = "sent"
                self._save_state()
                LOGGER.info(f"Processed and sent workbook for {date_str}")
            else:
                self.report = "error: processing failed"
        except Exception as e:
            self.report = f"error: {str(e)}"
            LOGGER.error(f"Failed to process/send for {date_str}: {e}")

    async def send_workbook(self, workbook_path, timestamp):
        """Send the daily workbook report via email."""
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
            LOGGER.info(f"Sent workbook to {', '.join(self.recipients)}")
        except Exception as e:
            LOGGER.error(f"Failed to send email: {e}")
            raise

    async def do_command(self, command: dict, *, timeout: float = None, **kwargs) -> dict:
        """Handle manual command execution."""
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
        elif command.get("command") == "process":
            day = command.get("day", datetime.datetime.now().strftime("%Y%m%d"))
            try:
                timestamp = datetime.datetime.strptime(day, "%Y%m%d")
                await self.process_workbook(timestamp, day)
                return {"status": f"Processed workbook for {day}"}
            except ValueError:
                return {"status": f"Invalid day format: {day}, use YYYYMMDD"}
            except Exception as e:
                return {"status": f"Error: {str(e)}"}
        elif command.get("command") == "send":
            day = command.get("day", datetime.datetime.now().strftime("%Y%m%d"))
            try:
                timestamp = datetime.datetime.strptime(day, "%Y%m%d")
                await self.send_processed_workbook(timestamp, day)
                return {"status": f"Sent processed workbook for {day}"}
            except ValueError:
                return {"status": f"Invalid day format: {day}, use YYYYMMDD"}
            except Exception as e:
                return {"status": f"Error: {str(e)}"}
        return {"status": "Unknown command"}

    async def get_readings(self, *, extra: dict = None, timeout: float = None, **kwargs) -> dict[str, SensorReading]:
        """Return the current state of the sensor for monitoring."""
        now = datetime.datetime.now()
        next_process = self._get_next_process_time(now)
        next_send = self._get_next_send_time(now)
        
        # Format dates as YYYYMMDD and times as full datetime string
        return {
            "status": "running",
            "last_processed_date": self.last_processed_date or "never",
            "last_processed_time": self.last_processed_time or "never",
            "last_sent_date": self.last_sent_date or "never",
            "last_sent_time": self.last_sent_time or "never",
            "next_process_date": next_process.strftime("%Y%m%d"),
            "next_process_time": str(next_process),
            "next_send_date": next_send.strftime("%Y%m%d"),
            "next_send_time": str(next_send),
            "report": self.report,
            "workbook": self.workbook,
            "data": self.data or "none",
            "pid": os.getpid(),
            "location": self.location
        }