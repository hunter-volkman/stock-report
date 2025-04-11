import asyncio
import os
import json
import datetime
from datetime import timedelta
import fasteners
import shutil
import zipfile
import xml.etree.ElementTree as ET
import openpyxl
from typing import Mapping, Optional, Any, Dict
from viam.module.module import Module
from viam.components.sensor import Sensor
from viam.proto.app.robot import ComponentConfig
from viam.resource.base import ResourceBase
from viam.resource.types import Model, ModelFamily
from viam.utils import SensorReading, struct_to_dict
from viam.logging import getLogger
from dateutil import tz
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Mail, Attachment, FileContent, FileName, 
    FileType, Disposition, Email, Content
)
import base64

from .export import DataExporter

LOGGER = getLogger(__name__)

class StockReportEmail(Sensor):
    """
    StockReportEmail component that generates and emails Excel workbook reports
    based on scheduled times from Viam API data, with optional image attachments.
    """
    
    MODEL = Model(ModelFamily("hunter", "stock-report"), "email")
    
    @classmethod
    def new(cls, config: ComponentConfig, dependencies: Mapping[str, ResourceBase]) -> "StockReportEmail":
        """Create a new StockReportEmail instance."""
        instance = cls(config.name)
        instance.reconfigure(config, dependencies)
        return instance
    
    @classmethod
    def validate_config(cls, config: ComponentConfig) -> list[str]:
        """Validate the configuration and return required dependencies."""
        attributes = struct_to_dict(config.attributes)
        
        # Check required attributes
        required = ["location", "recipients", "api_key_id", "api_key", "org_id", "sendgrid_api_key", "filename_prefix"]
        for attr in required:
            if attr not in attributes:
                raise ValueError(f"{attr} is required")
        
        # Validate send_time
        send_time = attributes.get("send_time", "20:00")
        try:
            datetime.datetime.strptime(str(send_time), "%H:%M")
        except ValueError:
            raise ValueError(f"Invalid send_time '{send_time}': must be in 'HH:MM' format")
            
        # Validate process_time if provided
        process_time = attributes.get("process_time")
        if process_time:
            try:
                datetime.datetime.strptime(str(process_time), "%H:%M")
            except ValueError:
                raise ValueError(f"Invalid process_time '{process_time}': must be in 'HH:MM' format")

        # Validate store hours
        for day in ["hours_mon", "hours_tue", "hours_wed", "hours_thu", "hours_fri", "hours_sat", "hours_sun"]:
            if day in attributes:
                hours = attributes[day]
                if not isinstance(hours, list) or len(hours) != 2:
                    raise ValueError(f"'{day}' must be a list with two elements: [opening_time, closing_time]")
                
                # Validate each time string
                for time_str in hours:
                    try:
                        datetime.datetime.strptime(str(time_str), "%H:%M")
                    except ValueError:
                        raise ValueError(f"Invalid time format in '{day}': '{time_str}' - must be in 'HH:MM' format")
        
        # Validate image_times if provided
        if "image_times" in attributes:
            image_times = attributes["image_times"]
            if not isinstance(image_times, list):
                raise ValueError("'image_times' must be a list")
            for time_str in image_times:
                try:
                    datetime.datetime.strptime(str(time_str), "%H:%M")
                except ValueError:
                    raise ValueError(f"Invalid time format in 'image_times': '{time_str}' - must be 'HH:MM'")
        
        # Return list of required dependencies (none for this component)
        return []
    
    def __init__(self, name: str):
        """Initialize the workbook report email component."""
        super().__init__(name)
        self.dependencies = {}
        self.config = None
        
        # Base configuration
        self.location = ""
        self.location_id = ""  # Add location_id
        self.filename_prefix = ""
        self.teleop_url = ""
        
        # Email configuration
        self.sendgrid_api_key = ""
        self.sender_email = "no-reply@viam.com"
        self.sender_name = "Workbook Report"
        self.recipients = []
        
        # API configuration
        self.api_key_id = ""
        self.api_key = ""
        self.org_id = ""
        
        # Scheduling
        self.send_time = "20:00"
        self.process_time = "19:00"  # Default to 1 hour before send
        self.timezone = "America/New_York"
        self.image_times = []  # List of times for image retrieval
        
        # Store hours
        self.hours_mon = ["07:00", "19:30"]
        self.hours_tue = ["07:00", "19:30"]
        self.hours_wed = ["07:00", "19:30"]
        self.hours_thu = ["07:00", "19:30"]
        self.hours_fri = ["07:00", "19:30"]
        self.hours_sat = ["08:00", "17:00"]
        self.hours_sun = ["08:00", "17:00"]
        
        # State
        self.last_processed_date = None
        self.last_processed_time = None
        self.last_sent_date = None
        self.last_sent_time = None
        self.data = None  # Path to the latest report file
        self.report = "not_sent"
        self.workbook = "not_processed"
        
        # Background tasks
        self.loop_task = None
        
        # State persistence - Similar to stock-alert module
        self.state_dir = os.path.join(os.path.expanduser("~"), ".stock-report")
        self.state_file = os.path.join(self.state_dir, f"{name}.json")
        self.workbooks_dir = os.path.join(self.state_dir, "workbooks")
        self.lock_file = f"{self.state_file}.lock"
        
        # Create necessary directories
        os.makedirs(self.state_dir, exist_ok=True)
        os.makedirs(self.workbooks_dir, exist_ok=True)
        
        # Load persisted state
        self._load_state()
        LOGGER.info(f"Initialized with PID: {os.getpid()}")
    
    def _load_state(self):
        """Load persistent state from file with locking."""
        if os.path.exists(self.state_file):
            # Use a file lock to ensure safe reads
            lock = fasteners.InterProcessLock(self.lock_file)
            
            try:
                # Acquire the lock with a timeout
                if lock.acquire(blocking=True, timeout=5):
                    try:
                        with open(self.state_file, "r") as f:
                            state = json.load(f)
                            self.last_processed_date = state.get("last_processed_date")
                            self.last_processed_time = state.get("last_processed_time")
                            self.last_sent_date = state.get("last_sent_date")
                            self.last_sent_time = state.get("last_sent_time")
                            self.data = state.get("data")
                            self.report = state.get("report", "not_sent")
                            self.workbook = state.get("workbook", "not_processed")
                        
                        LOGGER.info(f"Loaded state from {self.state_file}")
                    finally:
                        lock.release()
                else:
                    LOGGER.warning(f"Could not acquire lock to load state for {self.name}")
            except Exception as e:
                LOGGER.error(f"Error loading state: {e}")
        else:
            LOGGER.info(f"No state file at {self.state_file}, starting fresh")
    
    def _save_state(self):
        """Save state to file for persistence across restarts using file locking."""
        # Use a file lock to ensure safe writes
        lock = fasteners.InterProcessLock(self.lock_file)
        
        try:
            # Acquire the lock with a timeout
            if lock.acquire(blocking=True, timeout=5):
                try:
                    state = {
                        "last_processed_date": self.last_processed_date,
                        "last_processed_time": self.last_processed_time,
                        "last_sent_date": self.last_sent_date,
                        "last_sent_time": self.last_sent_time,
                        "data": self.data,
                        "report": self.report,
                        "workbook": self.workbook
                    }
                    
                    # First write to a temporary file
                    temp_file = f"{self.state_file}.tmp"
                    with open(temp_file, "w") as f:
                        json.dump(state, f)
                    
                    # Then atomically replace the original file
                    os.replace(temp_file, self.state_file)
                    
                    LOGGER.debug(f"Saved state to {self.state_file}")
                finally:
                    lock.release()
            else:
                LOGGER.warning(f"Could not acquire lock to save state for {self.name}")
        except Exception as e:
            LOGGER.error(f"Error saving state: {e}")
    
    def reconfigure(self, config: ComponentConfig, dependencies: Mapping[str, ResourceBase]):
        """Configure the component with updated settings."""
        # Store config and dependencies
        self.config = config
        self.dependencies = dependencies
        
        # Get configuration attributes
        attributes = struct_to_dict(config.attributes)
        
        # Configure from attributes
        self.location = config.attributes.fields["location"].string_value
        self.location_id = attributes.get("location_id", "")  # Add location_id from config
        self.filename_prefix = attributes.get("filename_prefix", "")
        self.teleop_url = attributes.get("teleop_url", "")
        
        # Email configuration
        self.sender_email = attributes.get("sender_email", "no-reply@viam.com")
        self.sender_name = attributes.get("sender_name", "Workbook Report")
        self.sendgrid_api_key = attributes.get("sendgrid_api_key", "")
        
        # Handle recipients (string or list)
        recipients = attributes.get("recipients", [])
        if isinstance(recipients, list):
            self.recipients = recipients
        elif isinstance(recipients, str):
            self.recipients = [r.strip() for r in recipients.split(",")]
        else:
            LOGGER.warning(f"Unexpected recipients format: {type(recipients)}")
            self.recipients = [str(recipients)]
        
        # API configuration
        self.api_key_id = attributes.get("api_key_id", "")
        self.api_key = attributes.get("api_key", "")
        self.org_id = attributes.get("org_id", "")
        
        # Scheduling
        self.send_time = attributes.get("send_time", "20:00")
        
        # If process_time is not specified, calculate it as 1 hour before send_time
        if "process_time" in attributes:
            self.process_time = attributes.get("process_time")
        else:
            # Calculate process_time as 1 hour before send_time
            send_dt = datetime.datetime.strptime(self.send_time, "%H:%M")
            process_dt = send_dt - timedelta(hours=1)
            self.process_time = process_dt.strftime("%H:%M")
        
        self.timezone = attributes.get("timezone", "America/New_York")
        self.image_times = attributes.get("image_times", [])
        
        # Configure store hours
        self.hours_mon = attributes.get("hours_mon", ["07:00", "19:30"])
        self.hours_tue = attributes.get("hours_tue", ["07:00", "19:30"])
        self.hours_wed = attributes.get("hours_wed", ["07:00", "19:30"])
        self.hours_thu = attributes.get("hours_thu", ["07:00", "19:30"])
        self.hours_fri = attributes.get("hours_fri", ["07:00", "19:30"])
        self.hours_sat = attributes.get("hours_sat", ["08:00", "17:00"])
        self.hours_sun = attributes.get("hours_sun", ["08:00", "17:00"])
        
        # Log configuration details
        LOGGER.info(f"Configured {self.name} for location '{self.location}'")
        LOGGER.info(f"Process time: {self.process_time}, Send time: {self.send_time}")
        LOGGER.info(f"Will send reports to: {', '.join(self.recipients)}")
        if self.image_times:
            LOGGER.info(f"Image times configured: {', '.join(self.image_times)}")
        
        if self.sendgrid_api_key:
            LOGGER.info("SendGrid API key configured")
        else:
            LOGGER.warning("No SendGrid API key configured")
        
        # Cancel existing task if any
        if self.loop_task and not self.loop_task.done():
            self.loop_task.cancel()
            
        # Start scheduled task
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
            LOGGER.info(f"Another instance running, exiting (PID {os.getpid()})")
            return
            
        try:
            LOGGER.info(f"Started scheduled loop with PID {os.getpid()}")
            
            while True:
                now = datetime.datetime.now()
                today_str = now.strftime("%Y%m%d")
                
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
                
        except asyncio.CancelledError:
            LOGGER.info("Scheduled loop cancelled")
            raise
        except Exception as e:
            LOGGER.error(f"Scheduled loop failed: {e}")
        finally:
            lock.release()
            LOGGER.info(f"Released lock, loop exiting (PID {os.getpid()})")
    
    async def process_workbook(self, timestamp, date_str):
        """
        Process the workbook for data from the specified date, creating a report.
        
        Args:
            timestamp: Datetime object representing the processing time
            date_str: String representing the date to process (YYYYMMDD)
        """
        try:
            # Parse target date
            target_date = datetime.datetime.strptime(date_str, "%Y%m%d")
            target_date = target_date.replace(tzinfo=tz.gettz(self.timezone))
            LOGGER.info(f"Processing data for target date: {target_date.strftime('%Y-%m-%d')}")
            
            # Define file paths
            template_path = os.path.join(self.workbooks_dir, "template.xlsx")
            raw_data_path = os.path.join(self.workbooks_dir, "raw_export.xlsx")
            
            # Verify template exists
            if not os.path.exists(template_path):
                LOGGER.error(f"Template file not found: {template_path}")
                self.workbook = "error: missing template"
                self._save_state()
                return None
            
            # Get store hours for the target date
            opening_time, closing_time = self._get_store_hours_for_date(target_date)
            
            # Create datetime objects for the store hours
            open_hour, open_minute = map(int, opening_time.split(':'))
            close_hour, close_minute = map(int, closing_time.split(':'))
            
            start_time = target_date.replace(hour=open_hour, minute=open_minute, second=0, microsecond=0)
            end_time = target_date.replace(hour=close_hour, minute=close_minute, second=0, microsecond=0)
            
            LOGGER.info(f"Exporting data from {start_time} to {end_time}")
            
            # Export raw data
            # exporter = DataExporter(self.api_key_id, self.api_key, self.org_id, self.timezone)
            exporter = DataExporter(self.api_key_id, self.api_key, self.org_id, self.location_id, self.timezone)
            await exporter.export_to_excel(
                raw_data_path,
                "langer_fill",
                start_time,
                end_time,
                bucket_period="PT5M",
                bucket_method="pct99",
                include_keys_regex=".*_raw",
                tab_name="RAW"
            )
            
            LOGGER.info(f"Raw data exported to {raw_data_path}")
            
            # Create WIP file with new naming convention
            wip_filename = f"{self.filename_prefix}_wip_{target_date.strftime('%Y%m%d')}.xlsx"
            wip_path = os.path.join(self.workbooks_dir, wip_filename)
            
            # Create final filename with new convention
            final_filename = f"{self.filename_prefix}_{target_date.strftime('%Y%m%d')}.xlsx"
            final_path = os.path.join(self.workbooks_dir, final_filename)
            
            # Copy template to WIP file
            shutil.copy(template_path, wip_path)
            
            # Process the raw data and update the workbook
            num_data_rows = self._update_raw_import_sheet(raw_data_path, wip_path)
            LOGGER.info(f"Updated Raw Import sheet with {num_data_rows} rows")
            
            # Fix the workbook
            self._fix_workbook(wip_path, num_data_rows, final_path)
            LOGGER.info(f"Created final report: {final_path}")
            
            # Clean up WIP file
            if os.path.exists(wip_path):
                os.remove(wip_path)
                LOGGER.info(f"Removed temporary WIP file: {wip_path}")
            
            # Update state
            self.data = final_path
            self.last_processed_date = date_str
            self.last_processed_time = str(timestamp)
            self.workbook = "processed"
            self._save_state()
            
            return final_path
            
        except Exception as e:
            LOGGER.error(f"Failed to process workbook: {e}")
            self.workbook = f"error: {str(e)}"
            self._save_state()
            return None
    
    def _get_store_hours_for_date(self, date):
        """Get store hours for the specified date."""
        # Get day of week (0=Monday, 6=Sunday)
        weekday = date.weekday()
        
        # Map weekday to store hours
        if weekday == 0:  # Monday
            return tuple(self.hours_mon)
        elif weekday == 1:  # Tuesday
            return tuple(self.hours_tue)
        elif weekday == 2:  # Wednesday
            return tuple(self.hours_wed)
        elif weekday == 3:  # Thursday
            return tuple(self.hours_thu)
        elif weekday == 4:  # Friday
            return tuple(self.hours_fri)
        elif weekday == 5:  # Saturday
            return tuple(self.hours_sat)
        else:  # Sunday
            return tuple(self.hours_sun)
    
    def _update_raw_import_sheet(self, raw_file, output_file):
        """
        Update the Raw Import sheet in the output file with data from the raw file.
        
        Args:
            raw_file: Path to the raw data Excel file
            output_file: Path to the output workbook
            
        Returns:
            Number of data rows copied
        """
        try:
            # Load data from raw export file
            LOGGER.info(f"Loading raw data from {raw_file}")
            raw_wb = openpyxl.load_workbook(raw_file, data_only=True)
            
            if "RAW" not in raw_wb.sheetnames:
                LOGGER.error(f"RAW sheet not found in exported data")
                raise ValueError("RAW sheet not found in exported data")
                
            raw_sheet = raw_wb["RAW"]
            
            # Get data from raw sheet
            data_rows = list(raw_sheet.iter_rows(min_row=2, values_only=True))
            
            LOGGER.info(f"Loaded {len(data_rows)} rows of data from raw export")
            
            # Open the output workbook
            LOGGER.info(f"Opening output workbook: {output_file}")
            output_wb = openpyxl.load_workbook(output_file)
            
            if "Raw Import" not in output_wb.sheetnames:
                LOGGER.error(f"Raw Import sheet not found in template")
                raise ValueError("Raw Import sheet not found in template")
                
            output_sheet = output_wb["Raw Import"]
            
            # Clear existing data from Raw Import sheet (keeping headers)
            LOGGER.info("Clearing existing data from Raw Import sheet")
            for row in output_sheet.iter_rows(min_row=2):
                for cell in row:
                    cell.value = None
            
            # Copy data to Raw Import sheet
            LOGGER.info("Copying data to Raw Import sheet")
            for r_idx, row_data in enumerate(data_rows, start=2):
                for c_idx, value in enumerate(row_data, start=1):
                    output_sheet.cell(row=r_idx, column=c_idx).value = value
            
            # Save the workbook
            LOGGER.info(f"Saving updated workbook to {output_file}")
            output_wb.save(output_file)
            
            LOGGER.info(f"Raw Import sheet updated with {len(data_rows)} rows of data")
            return len(data_rows)
            
        except Exception as e:
            LOGGER.error(f"Error updating Raw Import sheet: {e}")
            raise
    
    def _get_sheet_mappings(self, excel_path):
        """
        Extract the mapping of sheet names to their XML filenames.
        
        Args:
            excel_path: Path to the Excel file
            
        Returns:
            Dictionary mapping sheet names to XML filenames
        """
        temp_dir = os.path.join(self.workbooks_dir, "temp_excel")
        os.makedirs(temp_dir, exist_ok=True)
        
        try:
            with zipfile.ZipFile(excel_path, "r") as zip_ref:
                zip_ref.extractall(temp_dir)

            workbook_xml_path = os.path.join(temp_dir, "xl", "workbook.xml")
            rels_xml_path = os.path.join(temp_dir, "xl", "_rels", "workbook.xml.rels")

            if not os.path.exists(workbook_xml_path):
                raise FileNotFoundError(f"workbook.xml not found in {excel_path}")
            if not os.path.exists(rels_xml_path):
                raise FileNotFoundError(f"workbook.xml.rels not found in {excel_path}")

            sheet_mapping = {}

            # Parse workbook.xml to get sheet names and their relationship IDs
            wb_tree = ET.parse(workbook_xml_path)
            wb_root = wb_tree.getroot()
            
            # Handle Excel namespaces
            ns = {'ns': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main',
                  'r': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships'}
            
            sheet_rel_map = {}  # Map r:id to sheet name
            for sheet in wb_root.findall(".//ns:sheets/ns:sheet", ns):
                sheet_name = sheet.attrib["name"]
                sheet_rel_id = sheet.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]
                sheet_rel_map[sheet_rel_id] = sheet_name

            # Parse workbook.xml.rels to get sheet file paths
            rels_tree = ET.parse(rels_xml_path)
            rels_root = rels_tree.getroot()
            rels_ns = {'ns': 'http://schemas.openxmlformats.org/package/2006/relationships'}

            for rel in rels_root.findall(".//ns:Relationship", rels_ns):
                rel_id = rel.attrib["Id"]
                target = rel.attrib["Target"]
                
                if rel_id in sheet_rel_map and "worksheets" in target:
                    sheet_name = sheet_rel_map[rel_id]
                    sheet_mapping[sheet_name] = os.path.basename(target)

            LOGGER.info(f"Sheet mappings: {sheet_mapping}")
            return sheet_mapping
            
        except Exception as e:
            LOGGER.error(f"Error extracting sheet mappings: {e}")
            raise
        finally:
            # Clean up temporary directory
            if os.path.exists(temp_dir) and "temp_excel" not in excel_path:
                shutil.rmtree(temp_dir)
    
    def _fix_workbook(self, wip_path, num_data_rows, final_path):
        """
        Fix the workbook structure to handle row counts and formulas.
        
        Args:
            wip_path: Path to the WIP workbook
            num_data_rows: Number of data rows
            final_path: Path to save the final workbook
            
        Returns:
            Path to the fixed workbook
        """
        # Create a unique temp directory
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        temp_dir = os.path.join(self.workbooks_dir, f"temp_excel_{timestamp}")
        
        try:
            # Ensure WIP file exists
            if not os.path.exists(wip_path):
                LOGGER.error(f"WIP file not found: {wip_path}")
                raise FileNotFoundError(f"WIP file not found: {wip_path}")

            # Create a fresh temp directory
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            os.makedirs(temp_dir, exist_ok=True)
            LOGGER.info(f"Created temp directory: {temp_dir}")
            
            # Extract the Excel file
            LOGGER.info(f"Extracting WIP Excel file: {wip_path}")
            with zipfile.ZipFile(wip_path, "r") as zip_ref:
                zip_ref.extractall(temp_dir)
            
            # Get sheet mappings
            LOGGER.info("Obtaining sheet mappings")
            sheet_mappings = self._get_sheet_mappings(wip_path)
            
            # Define namespaces
            namespaces = {
                'ns': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main',
                'r': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships'
            }
            
            # Register namespaces for proper XML generation
            for prefix, uri in namespaces.items():
                ET.register_namespace(prefix, uri)
            # Register default namespace
            ET.register_namespace('', 'http://schemas.openxmlformats.org/spreadsheetml/2006/main')
            
            # Path to worksheets directory
            worksheets_dir = os.path.join(temp_dir, "xl", "worksheets")
            if not os.path.exists(worksheets_dir):
                LOGGER.error(f"Worksheets directory not found: {worksheets_dir}")
                raise FileNotFoundError(f"Worksheets directory not found: {worksheets_dir}")
            
            # Process each sheet that needs fixing
            sheets_to_process = ["Calibrated Values", "Bounded Calibrated", "Empty Shelf Tracker"]
            
            for sheet_name in sheets_to_process:
                if sheet_name not in sheet_mappings:
                    LOGGER.warning(f"Sheet '{sheet_name}' not found in workbook. Skipping...")
                    continue
                
                sheet_xml_path = os.path.join(worksheets_dir, sheet_mappings[sheet_name])
                if not os.path.exists(sheet_xml_path):
                    LOGGER.error(f"Sheet XML file not found: {sheet_xml_path}")
                    continue
                
                LOGGER.info(f"Processing sheet: {sheet_name}")
                
                # Parse sheet XML
                try:
                    tree = ET.parse(sheet_xml_path)
                    root = tree.getroot()
                    
                    # Find sheetData element
                    sheet_data = root.find(".//ns:sheetData", namespaces)
                    if sheet_data is None:
                        LOGGER.warning(f"No sheetData found in {sheet_name}, skipping modifications")
                        continue
                    
                    # Remove excess rows
                    rows_to_remove = []
                    for row in sheet_data.findall(".//ns:row", namespaces):
                        row_number = int(row.attrib.get("r", "0"))
                        if row_number > num_data_rows + 1:  # +1 for header row
                            rows_to_remove.append(row)

                    # Remove excess rows
                    if rows_to_remove:
                        first_row = rows_to_remove[0].attrib.get('r') if rows_to_remove else "N/A"
                        last_row = rows_to_remove[-1].attrib.get('r') if rows_to_remove else "N/A"
                        
                        for row in rows_to_remove:
                            sheet_data.remove(row)
        
                        LOGGER.info(f"Removed {len(rows_to_remove)} excess rows ({first_row} to {last_row}) from {sheet_name}")
                    
                    # Save the modified sheet XML
                    tree.write(sheet_xml_path, encoding="UTF-8", xml_declaration=True)
                    LOGGER.info(f"Saved modifications to {sheet_xml_path}")
                    
                except Exception as e:
                    LOGGER.error(f"Error processing sheet {sheet_name}: {e}")
                    raise
            
            # Create the final zip file (Excel file)
            LOGGER.info(f"Creating final Excel file: {final_path}")
            with zipfile.ZipFile(final_path, "w", zipfile.ZIP_DEFLATED) as zip_out:
                for root_dir, _, files in os.walk(temp_dir):
                    for file in files:
                        file_path = os.path.join(root_dir, file)
                        arcname = os.path.relpath(file_path, temp_dir)
                        zip_out.write(file_path, arcname)
            
            LOGGER.info(f"Successfully created final Excel file: {final_path}")
            return final_path
            
        except Exception as e:
            LOGGER.error(f"Error in fix_workbook method: {e}")
            raise
        finally:
            # Clean up temporary directory
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                LOGGER.info(f"Cleaned up temporary directory: {temp_dir}")
    
    async def send_processed_workbook(self, timestamp, date_str):
        """Send the previously processed workbook via email."""
        if not self.data or not os.path.exists(self.data):
            LOGGER.error("No processed workbook available to send")
            self.report = "error: no processed workbook"
            self._save_state()
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
        """Process and send the workbook immediately."""
        try:
            workbook_path = await self.process_workbook(timestamp, date_str)
            if workbook_path:
                await self.send_workbook(workbook_path, timestamp)
                self.last_sent_date = date_str
                self.last_sent_time = str(timestamp)
                self.report = "sent"
                self._save_state()
                LOGGER.info(f"Processed and sent workbook for {date_str}")
                return {"status": "success", "message": f"Processed and sent workbook for {date_str}"}
            else:
                self.report = "error: processing failed"
                return {"status": "error", "message": "Processing failed"}
        except Exception as e:
            self.report = f"error: {str(e)}"
            LOGGER.error(f"Failed to process/send for {date_str}: {e}")
            return {"status": "error", "message": str(e)}
    
    async def send_workbook(self, workbook_path, timestamp):
        """
        Send the workbook report via email using SendGrid, with optional image attachments.
        
        Args:
            workbook_path: Path to the workbook file
            timestamp: Datetime object representing the send time
        """
        if not self.sendgrid_api_key:
            LOGGER.error("No SendGrid API key configured")
            raise ValueError("No SendGrid API key configured")
    
        if not os.path.exists(workbook_path):
            LOGGER.error(f"Workbook file not found: {workbook_path}")
            raise FileNotFoundError(f"Workbook file not found: {workbook_path}")
        
        try:
            LOGGER.info(f"Preparing email with workbook: {os.path.basename(workbook_path)}")
            subject = f"Daily Report: {timestamp.strftime('%Y-%m-%d')} - {self.location}"
            teleop_url = self.teleop_url if hasattr(self, 'teleop_url') and self.teleop_url else "#"
            body_text = f"See the attached Excel with data for review. Click here for the link to the real-time view of the store: {teleop_url}"
            
            message = Mail(
                from_email=Email(self.sender_email, self.sender_name),
                to_emails=self.recipients,
                subject=subject,
                plain_text_content=Content("text/plain", body_text)
            )
            
            html_content = f"""<html>
    <body>
    <p>See the attached Excel with data for review. <a href="{teleop_url}">Click here</a> for the link to the real-time view of the store.</p>
    </body>
    </html>"""
            
            # Add images if configured
            attached_images = []
            if self.image_times:
                target_date = datetime.datetime.strptime(self.last_processed_date, "%Y%m%d").date()
                local_tz = tz.gettz(self.timezone)
                
                # Create a new client connection specifically for image retrieval
                LOGGER.info("Creating new connection for image retrieval")
                exporter = DataExporter(self.api_key_id, self.api_key, self.org_id, self.location_id, self.timezone)
                client = await exporter.connect()
                
                if client and client.data_client:
                    try:
                        # Get store hours and convert image times to datetime objects
                        opening_time, closing_time = self._get_store_hours_for_date(target_date)
                        open_hour, open_minute = map(int, opening_time.split(':'))
                        close_hour, close_minute = map(int, closing_time.split(':'))
                        
                        start_time = datetime.datetime.combine(target_date, datetime.time(open_hour, open_minute), tzinfo=local_tz)
                        end_time = datetime.datetime.combine(target_date, datetime.time(close_hour, close_minute), tzinfo=local_tz)
                        
                        desired_times = []
                        for time_str in self.image_times:
                            hour, minute = map(int, time_str.split(':'))
                            dt_local = datetime.datetime.combine(target_date, datetime.time(hour, minute), tzinfo=local_tz)
                            desired_times.append(dt_local)
                        
                        LOGGER.info(f"Fetching images for times: {', '.join(self.image_times)}")
                        
                        # Retrieve images one by one instead of all at once
                        images_dir = os.path.join(self.state_dir, "images")
                        os.makedirs(images_dir, exist_ok=True)
                        
                        for dt_local in desired_times:
                            try:
                                # Create filter for this specific time
                                from viam.proto.app.data import CaptureInterval, Order, Filter
                                from google.protobuf.timestamp_pb2 import Timestamp
                                
                                # Target 5 minutes before and after the desired time
                                start_dt = dt_local - datetime.timedelta(minutes=5)
                                end_dt = dt_local + datetime.timedelta(minutes=5)
                                
                                start_ts = Timestamp()
                                start_ts.FromDatetime(start_dt.astimezone(pytz.utc))
                                end_ts = Timestamp()
                                end_ts.FromDatetime(end_dt.astimezone(pytz.utc))
                                
                                capture_interval = CaptureInterval(start=start_ts, end=end_ts)
                                filter = Filter(
                                    component_name="ffmpeg",
                                    method="ReadImage",
                                    interval=capture_interval,
                                    organization_ids=[self.org_id]
                                )
                                
                                LOGGER.info(f"Looking for image around {dt_local.strftime('%H:%M')}")
                                
                                # Get images within this time window
                                images, _, _ = await client.data_client.binary_data_by_filter(
                                    filter=filter,
                                    include_binary_data=True,
                                    limit=1,
                                    sort_order=Order.ORDER_ASCENDING
                                )
                                
                                if images and len(images) > 0:
                                    # Save image to disk
                                    time_str = dt_local.strftime("%H%M")
                                    temp_image_path = os.path.join(images_dir, f"image_{time_str}.jpg")
                                    
                                    with open(temp_image_path, "wb") as f:
                                        f.write(images[0].binary)
                                    
                                    LOGGER.info(f"Saved image for {time_str} to {temp_image_path}")
                                    
                                    # Attach to email
                                    with open(temp_image_path, "rb") as f:
                                        file_content = base64.b64encode(f.read()).decode()
                                    
                                    filename = f"image_{time_str}.jpg"
                                    attachment = Attachment()
                                    attachment.file_content = FileContent(file_content)
                                    attachment.file_name = FileName(filename)
                                    attachment.file_type = FileType("image/jpeg")
                                    attachment.disposition = Disposition("attachment")
                                    message.add_attachment(attachment)
                                    
                                    attached_images.append(f"{filename} ({dt_local.strftime('%H:%M')})")
                                    LOGGER.info(f"Added image attachment: {filename}")
                                else:
                                    LOGGER.warning(f"No images found for time {dt_local.strftime('%H:%M')}")
                            
                            except Exception as e:
                                LOGGER.error(f"Error retrieving image for {dt_local.strftime('%H:%M')}: {e}")
                        
                    except Exception as e:
                        LOGGER.error(f"Error in image retrieval process: {e}")
                    finally:
                        # Always close the client when done
                        await client.close()
                else:
                    LOGGER.error("Failed to create client for image retrieval")
            
            # Update email content with image information
            if attached_images:
                images_text = "Attached images:\n" + "\n".join(attached_images)
                body_text += "\n\n" + images_text
                html_content = html_content.replace("</body>", f"<p>Attached images:<br>{'<br>'.join(attached_images)}</p></body>")
            
            message.add_content(Content("text/html", html_content))
            
            # Attach the workbook
            with open(workbook_path, "rb") as f:
                file_content = base64.b64encode(f.read()).decode()
            
            file_name = os.path.basename(workbook_path)
            attachment = Attachment()
            attachment.file_content = FileContent(file_content)
            attachment.file_name = FileName(file_name)
            attachment.file_type = FileType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            attachment.disposition = Disposition("attachment")
            message.add_attachment(attachment)
            
            # Send the email
            LOGGER.info(f"Sending email to {len(self.recipients)} recipients")
            sg = SendGridAPIClient(self.sendgrid_api_key)
            response = sg.send(message)
            
            LOGGER.info(f"Email sent via SendGrid API. Status code: {response.status_code}")
            return True
            
        except Exception as e:
            LOGGER.error(f"Failed to send email: {e}")
            raise
    
    async def get_readings(self, *, extra: Optional[Dict[str, Any]] = None, timeout: Optional[float] = None, **kwargs) -> Dict[str, SensorReading]:
        """Return the current state of the sensor for monitoring."""
        now = datetime.datetime.now()
        next_process = self._get_next_process_time(now)
        next_send = self._get_next_send_time(now)

        # Map store hours for display
        store_hours = {
            "monday": self.hours_mon,
            "tuesday": self.hours_tue,
            "wednesday": self.hours_wed,
            "thursday": self.hours_thu,
            "friday": self.hours_fri,
            "saturday": self.hours_sat,
            "sunday": self.hours_sun
        }
        
        return {
            "last_processed_date": self.last_processed_date or "never",
            "last_processed_time": self.last_processed_time or "never",
            "last_sent_date": self.last_sent_date or "never",
            "last_sent_time": self.last_sent_time or "never",
            "last_workbook_path": self.data or "none",
            "next_process_date": next_process.strftime("%Y%m%d"),
            "next_process_time": str(next_process),
            "next_send_date": next_send.strftime("%Y%m%d"),
            "next_send_time": str(next_send),
            "timezone": self.timezone,
            "filename_prefix": self.filename_prefix,
            "store_hours": store_hours,
            "image_times": self.image_times,
            "report": self.report,
            "pid": os.getpid(),
            "location": self.location
        }
    
    async def do_command(self, command: Dict[str, Any], *, timeout: Optional[float] = None, **kwargs) -> Dict[str, Any]:
        """Handle manual command execution."""
        cmd = command.get("command", "")
        
        if cmd == "process_and_send":
            day = command.get("day", datetime.datetime.now().strftime("%Y%m%d"))
            try:
                timestamp = datetime.datetime.strptime(day, "%Y%m%d")
                result = await self.process_and_send(timestamp, day)
                return result
            except ValueError:
                return {"status": "error", "message": f"Invalid day format: {day}, use YYYYMMDD"}
                
        elif cmd == "process":
            day = command.get("day", datetime.datetime.now().strftime("%Y%m%d"))
            try:
                timestamp = datetime.datetime.strptime(day, "%Y%m%d")
                final_path = await self.process_workbook(timestamp, day)
                if final_path:
                    return {"status": "success", "message": f"Processed workbook for {day}", "path": final_path}
                else:
                    return {"status": "error", "message": f"Failed to process workbook for {day}"}
            except ValueError:
                return {"status": "error", "message": f"Invalid day format: {day}, use YYYYMMDD"}
                
        elif cmd == "send":
            day = command.get("day", datetime.datetime.now().strftime("%Y%m%d"))
            try:
                timestamp = datetime.datetime.strptime(day, "%Y%m%d")
                await self.send_processed_workbook(timestamp, day)
                return {"status": "success", "message": f"Sent processed workbook for {day}"}
            except ValueError:
                return {"status": "error", "message": f"Invalid day format: {day}, use YYYYMMDD"}
                
        elif cmd == "test_email":
            try:
                if not self.sendgrid_api_key:
                    return {"status": "error", "message": "No SendGrid API key configured"}
                    
                # Create test email content
                timestamp = datetime.datetime.now()
                subject = f"Test Report: {timestamp.strftime('%Y-%m-%d')} - {self.location}"
                body = f"This is a test email from {self.name} at {self.location}.\nTime: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}"
                
                # Create email message
                message = Mail(
                    from_email=Email(self.sender_email, self.sender_name),
                    to_emails=self.recipients,
                    subject=subject,
                    plain_text_content=Content("text/plain", body)
                )
                
                # Add HTML content
                html_body = body.replace("\n", "<br>")
                message.add_content(Content("text/html", f"<html><body><p>{html_body}</p></body></html>"))
                
                # Send email
                sg = SendGridAPIClient(self.sendgrid_api_key)
                response = sg.send(message)
                
                return {
                    "status": "success",
                    "message": f"Test email sent with status code {response.status_code}",
                    "recipients": self.recipients
                }
            except Exception as e:
                return {"status": "error", "message": f"Failed to send test email: {str(e)}"}
        
        elif cmd == "get_schedule":
            now = datetime.datetime.now()
            next_process = self._get_next_process_time(now)
            next_send = self._get_next_send_time(now)
            
            return {
                "status": "success",
                "process_time": self.process_time,
                "send_time": self.send_time,
                "next_process": str(next_process),
                "next_send": str(next_send),
                "timezone": self.timezone,
                "image_times": self.image_times
            }
            
        else:
            return {"status": "error", "message": f"Unknown command: {cmd}"}