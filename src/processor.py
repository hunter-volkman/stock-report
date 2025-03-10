import subprocess
import os
import shutil
import tempfile
from datetime import datetime, timedelta
from dateutil import tz
import openpyxl
from openpyxl.utils import get_column_letter
from viam.logging import getLogger

LOGGER = getLogger(__name__)

class WorkbookProcessor:
    def __init__(self, work_dir, export_script, api_key_id, api_key, org_id, 
                 timezone="America/New_York", export_start_time="7:00", export_end_time="19:00"):
        self.work_dir = work_dir
        self.export_script = export_script
        self.api_key_id = api_key_id
        self.api_key = api_key
        self.org_id = org_id
        self.timezone = timezone
        self.export_start_time = export_start_time
        self.export_end_time = export_en
        # Check if LibreOffice is available for formula recalculation
        self.libreoffice_available = self._check_libreoffice()
        if self.libreoffice_available:
            LOGGER.info("LibreOffice is available for formula recalculation")
        else:
            LOGGER.warning("LibreOffice not found - complex formula recalculation may be limited")

    def _check_libreoffice(self):
        """Check if LibreOffice is available on the system."""
        try:
            result = subprocess.run(
                ["which", "libreoffice"], 
                capture_output=True, 
                text=True, 
                check=False
            )
            return result.returncode == 0
        except Exception:
            return False

    def get_yesterday_date(self):
        """Get yesterday's date in the configured timezone."""
        now = datetime.now(tz.gettz(self.timezone))
        return now - timedelta(days=1)

    def run_vde_export(self, output_file):
        """Run the vde.py script to export raw data for yesterday."""
        yesterday = self.get_yesterday_date()
        
        # Parse the time strings into hours and minutes
        start_hour, start_minute = map(int, self.export_start_time.split(':'))
        end_hour, end_minute = map(int, self.export_end_time.split(':'))
        
        # Create the datetime objects for start and end times
        start_time = yesterday.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0)
        end_time = yesterday.replace(hour=end_hour, minute=end_minute, second=0, microsecond=0)

        LOGGER.info(f"Exporting data from {start_time} to {end_time} ({self.export_start_time} to {self.export_end_time})")
        
        cmd = [
            "python3", self.export_script, "-vv", "excel",
            "--apiKeyId", self.api_key_id,
            "--apiKey", self.api_key,
            "--orgId", self.org_id,
            "--resourceName", "langer_fill",
            "--start", start_time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "--end", end_time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "--timezone", self.timezone,
            "--bucketPeriod", "PT1M",
            "--bucketMethod", "max",
            "--includeKeys", ".*_raw",
            "--output", output_file,
            "--tab", "RAW"
        ]

        try:
            # Run in the script's directory to handle relative paths
            LOGGER.info(f"Running vde.py command: {' '.join(cmd)}")
            process = subprocess.run(
                cmd, 
                check=True, 
                cwd=os.path.dirname(self.export_script),
                capture_output=True,
                text=True
            )
            
            # Log important parts of the output, but not everything to avoid spamming logs
            stdout_lines = process.stdout.strip().split('\n')
            if stdout_lines:
                # Log first 5 lines and last 5 lines if there's a lot of output
                if len(stdout_lines) > 10:
                    LOGGER.info("vde.py output first lines:")
                    for line in stdout_lines[:5]:
                        LOGGER.info(f"  {line}")
                    LOGGER.info("...")
                    LOGGER.info("vde.py output last lines:")
                    for line in stdout_lines[-5:]:
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

    def _recalculate_with_libreoffice(self, excel_file):
        """Use LibreOffice to ensure formulas are recalculated."""
        if not self.libreoffice_available:
            LOGGER.warning("Skipping LibreOffice recalculation (not available)")
            return
            
        try:
            # Create a temporary directory for the conversion
            with tempfile.TemporaryDirectory() as temp_dir:
                # Get filename without path
                filename = os.path.basename(excel_file)
                
                # Construct LibreOffice command for silent recalculation
                cmd = [
                    "libreoffice", "--headless", "--calc", 
                    "--convert-to", "xlsx", 
                    "--outdir", temp_dir,
                    excel_file
                ]
                
                LOGGER.info(f"Recalculating formulas in {excel_file} with LibreOffice")
                result = subprocess.run(cmd, capture_output=True, text=True, check=False)
                
                if result.returncode != 0:
                    LOGGER.warning(f"LibreOffice recalculation warning: {result.stderr}")
                
                # Get the converted file name
                converted_file = os.path.join(temp_dir, filename)
                
                # If the conversion was successful, copy back the recalculated file
                if os.path.exists(converted_file):
                    shutil.copy(converted_file, excel_file)
                    LOGGER.info(f"Recalculated file saved back to {excel_file}")
                else:
                    LOGGER.warning(f"LibreOffice did not create converted file: {os.listdir(temp_dir)}")
        except Exception as e:
            LOGGER.error(f"Error during LibreOffice recalculation: {e}")

    def update_master_workbook(self, raw_file, master_template):
        """Copy data from raw export to master workbook and update formulas."""
        yesterday = self.get_yesterday_date()
        today_str = datetime.now(tz.gettz(self.timezone)).strftime("%m%d%y")
        new_master_file = os.path.join(self.work_dir, f"3895th_{today_str}.xlsx")

        LOGGER.info(f"Creating new master workbook: {new_master_file}")
        shutil.copy(master_template, new_master_file)
        
        try:
            # Use openpyxl for data transfer
            raw_wb = openpyxl.load_workbook(raw_file)
            master_wb = openpyxl.load_workbook(new_master_file, data_only=False)  # Keep formulas

            # Check if RAW sheet exists in raw workbook
            if "RAW" not in raw_wb.sheetnames:
                LOGGER.error(f"'RAW' sheet not found in raw export workbook")
                raise ValueError("Raw export workbook missing 'RAW' sheet")
                
            raw_sheet = raw_wb["RAW"]
            
            # Check if "Raw Import" tab exists in master workbook
            if "Raw Import" not in master_wb.sheetnames:
                LOGGER.error(f"'Raw Import' sheet not found in master workbook")
                raise ValueError("Master workbook missing 'Raw Import' sheet")
                
            import_sheet = master_wb["Raw Import"]

            # Clear existing data in the import sheet
            LOGGER.info("Clearing existing data in Raw Import sheet")
            for row in import_sheet.iter_rows():
                for cell in row:
                    cell.value = None

            # Copy data more efficiently using cell references
            LOGGER.info("Copying data from RAW to Raw Import")
            row_count = 0
            for row_idx, row in enumerate(raw_sheet.rows, start=1):
                for col_idx, cell in enumerate(row, start=1):
                    import_sheet.cell(row=row_idx, column=col_idx).value = cell.value
                row_count += 1

            # Save the workbook with updated data
            master_wb.save(new_master_file)
            LOGGER.info(f"Copied {row_count} rows to Raw Import tab")
            
            # Use LibreOffice for formula recalculation if available
            self._recalculate_with_libreoffice(new_master_file)
            
            LOGGER.info(f"Updated workbook saved at {new_master_file}")
            return new_master_file
        except Exception as e:
            LOGGER.error(f"Error updating master workbook: {e}")
            if os.path.exists(new_master_file):
                try:
                    # Try to keep the file for troubleshooting
                    error_file = f"{new_master_file}.error"
                    shutil.copy(new_master_file, error_file)
                    LOGGER.info(f"Saved error state to {error_file}")
                except Exception:
                    pass
            raise

    def process(self, master_template):
        """Main processing function: run export, update master workbook."""
        os.makedirs(self.work_dir, exist_ok=True)
        LOGGER.info(f"Starting workbook processing with template {master_template}")
        
        raw_file = os.path.join(self.work_dir, "raw_export.xlsx")
        self.run_vde_export(raw_file)
        return self.update_master_workbook(raw_file, master_template)