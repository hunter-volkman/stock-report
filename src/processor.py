import subprocess
import os
import shutil
from datetime import datetime, timedelta
from dateutil import tz
import openpyxl
from openpyxl.utils import get_column_letter
from viam.logging import getLogger

LOGGER = getLogger(__name__)

class WorkbookProcessor:
    def __init__(self, work_dir, export_script, api_key_id, api_key, org_id, timezone="America/New_York"):
        self.work_dir = work_dir
        self.export_script = export_script
        self.api_key_id = api_key_id
        self.api_key = api_key
        self.org_id = org_id
        self.timezone = timezone

    def get_yesterday_date(self):
        now = datetime.now(tz.gettz(self.timezone))
        return now - timedelta(days=1)

    def run_vde_export(self, output_file):
        yesterday = self.get_yesterday_date()
        start_time = yesterday.replace(hour=7, minute=0, second=0, microsecond=0)
        end_time = yesterday.replace(hour=19, minute=0, second=0, microsecond=0)

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
            subprocess.run(cmd, check=True, cwd=os.path.dirname(self.export_script))
            if not os.path.exists(output_file):
                raise FileNotFoundError("vde.py ran but raw_export.xlsx was not created.")
            LOGGER.info(f"Generated raw data at {output_file}")
            return output_file
        except subprocess.CalledProcessError as e:
            LOGGER.error(f"Failed to run vde.py: {e}")
            raise

    def update_master_workbook(self, raw_file, master_template):
        yesterday = self.get_yesterday_date()
        today_str = datetime.now(tz.gettz(self.timezone)).strftime("%m%d%y")
        new_master_file = os.path.join(self.work_dir, f"3895th_{today_str}.xlsx")

        shutil.copy(master_template, new_master_file)
        raw_wb = openpyxl.load_workbook(raw_file)
        master_wb = openpyxl.load_workbook(new_master_file)

        raw_sheet = raw_wb["RAW"]
        import_sheet = master_wb["Raw Import"]

        for row in import_sheet.iter_rows():
            for cell in row:
                cell.value = None

        for row_idx, row in enumerate(raw_sheet.iter_rows(values_only=True), start=1):
            for col_idx, value in enumerate(row, start=1):
                import_sheet[f"{get_column_letter(col_idx)}{row_idx}"].value = value

        master_wb.save(new_master_file)
        LOGGER.info(f"Updated workbook saved at {new_master_file}")
        return new_master_file

    def process(self, master_template):
        os.makedirs(self.work_dir, exist_ok=True)
        raw_file = os.path.join(self.work_dir, "raw_export.xlsx")
        self.run_vde_export(raw_file)
        return self.update_master_workbook(raw_file, master_template)