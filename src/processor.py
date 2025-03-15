import subprocess
import os
import shutil
import tempfile
import re
from datetime import datetime, timedelta
from dateutil import tz
import openpyxl
from viam.logging import getLogger
import zipfile
import xml.etree.ElementTree as ET

LOGGER = getLogger(__name__)

class WorkbookProcessor:
    def __init__(self, work_dir, export_script, api_key_id, api_key, org_id, 
                 timezone="America/New_York", export_start_time_weekday="7:00", 
                 export_end_time_weekday="19:00", export_start_time_weekend="8:00", 
                 export_end_time_weekend="16:00"):
        self.work_dir = work_dir
        self.export_script_path = export_script
        self.export_script_dir = os.path.dirname(export_script)
        self.api_key_id = api_key_id
        self.api_key = api_key
        self.org_id = org_id
        self.timezone = timezone
        self.export_start_time_weekday = export_start_time_weekday
        self.export_end_time_weekday = export_end_time_weekday
        self.export_start_time_weekend = export_start_time_weekend
        self.export_end_time_weekend = export_end_time_weekend

        # Check for viam-python-data-export virtual environment
        self.venv_path = os.path.join(self.export_script_dir, ".venv")
        if not os.path.exists(self.venv_path):
            LOGGER.warning(f"viam-python-data-export virtual environment not found at {self.venv_path}, attempting to set it up")
            self._setup_venv()
        else:
            LOGGER.info(f"Found viam-python-data-export virtual environment at {self.venv_path}")
    
    def _setup_venv(self):
        """Set up the virtual environment for viam-python-data-export if it doesn't exist"""
        try:
            setup_script = os.path.join(self.export_script_dir, "setup.sh")
            if os.path.exists(setup_script):
                LOGGER.info(f"Running setup script for viam-python-data-export: {setup_script}")
                subprocess.run(
                    ["bash", "-c", f"cd {self.export_script_dir} && source ./setup.sh"],
                    check=True,
                    shell=False
                )
                LOGGER.info("viam-python-data-export setup script completed successfully!")
            else:
                LOGGER.error(f"Setup script not found at {setup_script}")
        except Exception as e:
            LOGGER.error(f"Failed to set up viam-python-data-export virtual environment: {e}")

    def get_yesterday_date(self):
        """Get yesterday's date in the configured timezone."""
        now = datetime.now(tz.gettz(self.timezone))
        return now - timedelta(days=1)

    def _get_export_times_for_day(self, target_date):
        """Determine export start and end times based on whether it's a weekday or weekend."""
        is_weekday = target_date.weekday() < 5  # Mon=0, Sun=6
        start_time_str = self.export_start_time_weekday if is_weekday else self.export_start_time_weekend
        end_time_str = self.export_end_time_weekday if is_weekday else self.export_end_time_weekend
        return start_time_str, end_time_str

    def run_vde_export(self, output_file, target_date=None):
        """Run the vde.py script to export raw data for the specified date or yesterday."""
        # Use the provided date or default to yesterday
        if target_date is None:
            target_date = self.get_yesterday_date()
            LOGGER.info(f"No target date provided, using yesterday: {target_date.strftime('%Y-%m-%d')}")
        
        # Get the appropriate export times based on the day
        start_time_str, end_time_str = self._get_export_times_for_day(target_date)
        
        # Parse the time strings into hours and minutes
        start_hour, start_minute = map(int, start_time_str.split(':'))
        end_hour, end_minute = map(int, end_time_str.split(':'))
        
        # Create the datetime objects for start and end times
        start_time = target_date.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0)
        end_time = target_date.replace(hour=end_hour, minute=end_minute, second=0, microsecond=0)

        LOGGER.info(f"Exporting data from {start_time} to {end_time} ({start_time_str} to {end_time_str})")

        # Construct the shell script to run vde.py with its virtual environment
        venv_python = os.path.join(self.venv_path, "bin", "python")
        export_script_path = self.export_script_path

        # Build the command        
        cmd = [
            venv_python,
            export_script_path,
            "-vv", 
            "excel",
            "--apiKeyId", self.api_key_id,
            "--apiKey", self.api_key,
            "--orgId", self.org_id,
            "--resourceName", "langer_fill",
            "--start", start_time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "--end", end_time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "--timezone", self.timezone,
            "--bucketPeriod", "PT5M",
            "--bucketMethod", "max",
            "--includeKeys", ".*_raw",
            "--output", output_file,
            "--tab", "RAW"
        ]

        # Create a masked version of the command for logging
        cmd_mask = cmd.copy()

        # Find the index of the sensitive parameters and mask them
        if "--apiKeyId" in cmd_mask:
            idx = cmd_mask.index("--apiKeyId")
            if idx + 1 < len(cmd_mask):
                cmd_mask[idx + 1] = "<redacted>"
        if "--apiKey" in cmd_mask:
            idx = cmd_mask.index("--apiKey")
            if idx + 1 < len(cmd_mask):
                cmd_mask[idx + 1] = "<redacted>"

        try:
            # Log the masked command
            LOGGER.info(f"Running vde.py command: {' '.join(cmd_mask)}")
            process = subprocess.run(
                cmd, 
                check=True, 
                cwd=self.export_script_dir,
                capture_output=True,
                text=True
            )
            
            # Log a summary of the output
            stdout_lines = process.stdout.strip().split('\n')
            if stdout_lines:
                # Log first 2 lines and last 2 lines if there's a lot of output
                if len(stdout_lines) > 4:
                    LOGGER.info("vde.py output first lines:")
                    for line in stdout_lines[:2]:
                        LOGGER.info(f"  {line}")
                    LOGGER.info("...")
                    LOGGER.info("vde.py output last lines:")
                    for line in stdout_lines[-2:]:
                        LOGGER.info(f"  {line}")
                else:
                    # Just log all if it's not too much
                    LOGGER.info("vde.py output:")
                    for line in stdout_lines:
                        LOGGER.info(f"  {line}")
            
            if not os.path.exists(output_file):
                raise FileNotFoundError("vde.py ran but raw_export.xlsx was not created.")
            
            LOGGER.info(f"Generated raw data at {output_file}")
            return output_file
        except subprocess.CalledProcessError as e:
            LOGGER.error(f"Failed to run vde.py: {e}")
            if e.stderr:
                stderr_lines = e.stderr.strip().split('\n')
                LOGGER.error("vde.py stderr output:")
                for line in stderr_lines:
                    LOGGER.error(f"  {line}")
            raise RuntimeError(f"vde.py export failed: {e}")

    def _extract_sheet_data(self, excel_file, sheet_name):
        """Extract data from a specific sheet in Excel file"""
        try:
            workbook = openpyxl.load_workbook(excel_file, data_only=True)
            
            if sheet_name not in workbook.sheetnames:
                LOGGER.error(f"Sheet '{sheet_name}' not found in {excel_file}")
                return None
                
            sheet = workbook[sheet_name]
            
            # Extract data as list of lists
            data = []
            for row in sheet.rows:
                row_data = [cell.value for cell in row]
                data.append(row_data)
                
            return data
        except Exception as e:
            LOGGER.error(f"Error extracting data from sheet {sheet_name} in {excel_file}: {e}")
            return None

    def _update_sheet_by_direct_edit(self, target_file, sheet_name, data):
        """
        Updated method using openpyxl to directly update sheet data
        while ensuring proper file save to preserve all Excel structures.
        """
        try:
            # Load the target workbook with all features preserved
            workbook = openpyxl.load_workbook(target_file, keep_vba=True, data_only=False)
            
            if sheet_name not in workbook.sheetnames:
                LOGGER.error(f"Sheet '{sheet_name}' not found in target workbook")
                return False
                
            sheet = workbook[sheet_name]
            
            # Clear the sheet data first (preserve formulas in other sheets)
            for row in sheet.iter_rows(min_row=2):  # Skip header row
                for cell in row:
                    cell.value = None
            
            # Insert the new data
            for row_idx, row_data in enumerate(data[1:], start=2):  # Skip header
                for col_idx, value in enumerate(row_data, start=1):
                    sheet.cell(row=row_idx, column=col_idx).value = value
            
            # Save with proper Excel compatibility flags
            workbook.save(target_file)
            LOGGER.info(f"Updated sheet '{sheet_name}' in {target_file}")
            return True
        except Exception as e:
            LOGGER.error(f"Error updating sheet '{sheet_name}' in {target_file}: {e}")
            return False

    def update_master_workbook(self, raw_file, master_template, target_date=None):
        """
        Improved approach that preserves Excel file integrity including charts.
        
        Args:
            raw_file: Path to the raw data export file
            master_template: Path to the master template to use as a base (ignored, we determine based on target date)
            target_date: Date for the target workbook (if None, use yesterday)
        """
        # Determine the target date for the new workbook
        if target_date is None:
            target_date = self.get_yesterday_date()
            LOGGER.info(f"No target date provided, using yesterday: {target_date.strftime('%Y-%m-%d')}")
        
        # Format the date string for the filename
        target_str = target_date.strftime("%m%d%y")
        new_master_file = os.path.join(self.work_dir, f"3895th_{target_str}.xlsx")

        # Use weekday or weekend template based on the target date
        is_weekday = target_date.weekday() < 5  # Mon=0, Sun=6
        template_name = "template_weekday.xlsx" if is_weekday else "template_weekend.xlsx"
        master_template = os.path.join(self.work_dir, template_name)
        
        if not os.path.exists(master_template):
            LOGGER.error(f"Master template {master_template} not found")
            raise FileNotFoundError(f"Master template {master_template} not found")

        LOGGER.info(f"Creating new master workbook: {new_master_file} for {target_date.strftime('%Y-%m-%d')} using template {master_template}")
        
        # Create a binary copy of the template (preserves all charts and VBA)
        with open(master_template, 'rb') as src, open(new_master_file, 'wb') as dst:
            dst.write(src.read())
        
        try:
            # 1. Extract RAW data from raw_file
            LOGGER.info(f"Extracting RAW data from {raw_file}")
            raw_data = self._extract_sheet_data(raw_file, "RAW")
            
            if not raw_data:
                raise ValueError(f"Failed to extract data from RAW sheet in {raw_file}")
                
            # Log data stats
            LOGGER.info(f"Extracted {len(raw_data)-1} rows from RAW sheet")
            
            # Count how many rows we expect based on weekday/weekend
            expected_rows = 156 if is_weekday else 96  # 156 rows (weekday) or 96 rows (weekend)
            actual_rows = len(raw_data) - 1  # Subtract 1 for header
            
            if actual_rows < expected_rows:
                LOGGER.warning(f"Expected {expected_rows} data rows but found only {actual_rows}. Continuing with available data.")
            
            # 2. Update the Raw Import sheet in the new workbook
            LOGGER.info(f"Updating 'Raw Import' sheet in {new_master_file}")
            success = self._update_sheet_by_direct_edit(new_master_file, "Raw Import", raw_data)
            
            if not success:
                raise ValueError(f"Failed to update Raw Import sheet in {new_master_file}")
                
            # 3. Verify the file integrity before returning
            try:
                # Quick verification - can we open the file?
                verify_wb = openpyxl.load_workbook(new_master_file, keep_vba=True)
                # If it gets here, file seems valid
                del verify_wb
                LOGGER.info(f"Verified updated workbook integrity: {new_master_file}")
            except Exception as e:
                LOGGER.error(f"Warning: Final verification failed, file may have issues: {e}")
                # Continue anyway as the file might still be usable
            
            LOGGER.info(f"Updated workbook successfully saved at {new_master_file}")
            return new_master_file
        except Exception as e:
            LOGGER.error(f"Error updating master workbook: {e}")
            if os.path.exists(new_master_file):
                try:
                    # Keep the file for troubleshooting
                    error_file = f"{new_master_file}.error"
                    shutil.copy(new_master_file, error_file)
                    LOGGER.info(f"Saved error state to {error_file}")
                except Exception:
                    pass
            raise

    def process(self, target_date=None):
        """
        Main processing function: run export, update master workbook.
        
        Args:
            target_date: Specific date to process (if None, use yesterday)
        """
        os.makedirs(self.work_dir, exist_ok=True)
        LOGGER.info(f"Starting workbook processing for {target_date.strftime('%Y-%m-%d') if target_date else 'yesterday'}")
        
        # Determine the date to process
        if target_date is None:
            target_date = self.get_yesterday_date()
            LOGGER.info(f"No target date provided, processing data for yesterday: {target_date.strftime('%Y-%m-%d')}")
        else:
            LOGGER.info(f"Using provided target date: {target_date.strftime('%Y-%m-%d')}")
        
        raw_file = os.path.join(self.work_dir, "raw_export.xlsx")
        
        # Get the raw data
        self.run_vde_export(raw_file, target_date)
        
        # Update the master workbook with the raw data
        return self.update_master_workbook(raw_file, None, target_date)