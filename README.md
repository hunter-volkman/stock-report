# Module workbook-emailer 

This module enables a Raspberry Pi to autonomously generate daily Excel workbook reports from sensor data, process them using a master template, and email them to specified recipients. It integrates with Viam’s data export tool (`vde.py`), persists its state for reliability across restarts, and operates independently of the Viam app’s CONTROL tab. Twice-daily `viam-agent` restarts (6:00 AM and 6:00 PM EST) ensure stability despite updates or connection issues.

## Model `hunter:sensor:workbook-emailer`

A custom sensor component that processes Excel workbooks with data from the viam-python-data-export tool. It runs locally on a Raspberry Pi, connects to a store's Viam machine, and uses a scheduled loop with inter-process locking for reliability.

### Configuration
Configure the model using the following JSON template in your Viam robot configuration:

```json
{
  "email": "<string>",
  "password": "<string>",
  "recipients": ["<string>", "<string>"],
  "location": "<string>",
  "api_key_id": "<string>",
  "api_key": "<string>",
  "org_id": "<string>",
  "send_time": "<string>",
  "process_time": "<string>",
  "export_start_time": "<string>",
  "export_end_time": "<string>",
  "timezone": "<string>",
  "save_dir": "<string>",
  "export_script": "<string>"
}
```

#### Attributes

The following attributes are available for this model:

| Name          | Type   | Inclusion | Description                |
|---------------|--------|-----------|----------------------------|
| `email` | string | Required | Gmail address for sending emails. |
| `password` | string | Required | Gmail App Password for authentication (generate via Google Account settings). |
| `recipients` | list of string | Required | Email addresses to receive the daily report. |
| `location` | string | Required | Location identifier for the email subject and body. |
| `api_key` | string | Required | Viam API key for data export via `vde.py`. |
| `api_key_id` | string | Required | Viam API key ID for data export via `vde.py`. |
| `org_id` | string | Required | Viam organization ID for data export via `vde.py`. |
| `send_time` | string | Optional | Time in `timezone` (`"HH:MM"`) to send the report. Defaults to `"20:00"`. |
| `process_time` | string | Optional | Time in `timezone` (`"HH:MM"`) to process the workbook. Defaults to 1 hour before `send_time`. |
| `export_start_time` | string | Optional | Start time in `timezone` (`"HH:MM"`) for data export. Defaults to `"7:00"`. |
| `export_end_time` | string | Optional | End time in `timezone` (`"HH:MM"`) for data export. Defaults to `"19:00"`. |
| `timezone` | string | Optional | Timezone for scheduling. Defaults to `"America/New_York"`. |
| `save_dir` | string | Optional | Directory to save workbooks. Defaults to `"/home/hunter.volkman/workbooks"`. |
| `export_script` | string | Optional | Path to `vde.py`. Defaults to `"/home/hunter.volkman/viam-python-data-export/vde.py"`. |


#### Example Configuration

```json
{
  "email": "user.name@example.com",
  "password": "gmail-app-password",
  "recipients": ["recipient1@example.com", "recipient2@example.com"],
  "location": "Test Location",
  "api_key_id": "<your-api-key-id>",
  "api_key": "<your-api-key>",
  "org_id": "<your-org-id>",
  "send_time": "22:00",
  "process_time": "21:00",
  "export_start_time": "7:00",
  "export_end_time": "19:00",
  "timezone": "America/New_York",
  "save_dir": "/home/user.name/workbooks",
  "export_script": "/home/user.name/viam-python-data-export/vde.py"
}
```

### Setup Instructions

1. **Install Dependencies**: 
  * Run `./setup.sh` to create a virtual environment and install requirements (`viam-sdk`, `openpyxl`, `python-dateutil`, `fasteners`, `typing-extensions`).
  * Ensure `viam-python-data-export` is installed at the `export_script` path (e.g., run its `setup.sh` if needed).
2. **Run the Module**: 
  * Execute `./run.sh` to start the module.
3. **Setup Daily Restarts**:
  * Create a restart script at `/home/user.name/scripts/restart.sh`:
```bash
#!/bin/bash
LOG_FILE="/home/user/scripts/restart.log"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
mkdir -p "$(dirname "$LOG_FILE")"
echo "[$TIMESTAMP] Starting viam-agent restart" >> "$LOG_FILE"
if sudo systemctl restart viam-agent >> "$LOG_FILE" 2>&1; then
    echo "[$TIMESTAMP] viam-agent restarted successfully" >> "$LOG_FILE"
else
    echo "[$TIMESTAMP] ERROR: viam-agent restart failed" >> "$LOG_FILE"
    exit 1
fi
sleep 5
if systemctl is-active viam-agent | grep -q "active"; then
    echo "[$TIMESTAMP] viam-agent confirmed running" >> "$LOG_FILE"
else
    echo "[$TIMESTAMP] ERROR: viam-agent not running after restart" >> "$LOG_FILE"
    exit 1
fi
```
  * Make it executable: `chmod +x /home/user.name/scripts/restart.sh`
  * Add to root’s crontab (`sudo crontab -e`):
```text
0 6 * * * /home/user.name/scripts/restart.sh
0 18 * * * /home/user.name/scripts/restart.sh
```
  * This restarts `viam-agent` daily at 6:00 AM EST and 6:00 PM EST.
4. **Test Configuration**:
* Ensure a master template (e.g., `3895th_031025.xlsx`) exists in `save_dir` with a "Raw Import" tab.
* Verify `export_script` points to a working `vde.py`.
* Check email delivery and workbook processing.

### Notes

* **Processing Logic**:
  * At `process_time` (e.g., `"21:00"`), fetches data from `export_start_time` to `export_end_time` (e.g., 7:00–19:00) for the previous day via `vde.py`.
  * Updates the "Raw Import" tab of the previous day’s master template (e.g., `3895th_031025.xlsx`) and saves as a new file (e.g., `3895th_031125.xlsx`).

* **Email Report**:
  * Sent at `send_time` (e.g., `"22:00"`), attaching the processed workbook (e.g., `3895th_031125.xlsx`).
  * Subject: `"Daily Fill Report - <location> - YYYY-MM-DD"`.

* **Workbook Storage**:
  * Saved in `save_dir` with filenames like `3895th_MMDDYY.xlsx`. Retained until manually deleted.

* **Resilience**:
  * Persists state in `state.json` (in `save_dir`) to track `last_processed_date`, `last_sent_date`, etc., preventing duplicates or missed actions.
  * Uses inter-process locking to avoid duplicate instances.
  * Twice-daily `viam-agent` restarts ensure stability.

* **Logging**:
  * Module logs: `viam logs` or `journalctl -u viam-agent`.
  * Restart logs: `/home/user/scripts/restart.log`.

### Example Logs

On restart:
```text
Reconfigured sensor-2 with save_dir: /home/hunter.volkman/workbooks, recipients: ["recipient@example.com"], location: Test Location, process_time: 21:00, send_time: 22:00
```

During processing:
```text
Processing workbook using template /home/hunter.volkman/workbooks/3895th_031025.xlsx for date 2025-03-11
Successfully processed workbook for 20250311, saved at /home/hunter.volkman/workbooks/3895th_031125.xlsx
```

During sending:
```text
Sent processed workbook for 20250311
```


### DoCommand

Supports manual operations via `do_command`:

* **Process and Send**:
```json
{"command": "process_and_send", "day": "20250311"}
```
* Processes and sends a report for the specified day (YYYYMMDD)

* **Process Only**:
```json
{"command": "process", "day": "20250311"}
```
* Processes the workbook for the specified day (YYYYMMDD).

* **Send Only**:
```json
{"command": "send", "day": "20250311"}
```
* Sends the processed workbook for the specified day (YYYYMMDD).