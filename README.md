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
| `send_time` | string | Optional | Viam organization ID for data export via `vde.py`. |




#### Example Configuration

```json
{
  "attribute_1": 1.0,
  "attribute_2": "foo"
}
```

### DoCommand

If your model implements DoCommand, provide an example payload of each command that is supported and the arguments that can be used. If your model does not implement DoCommand, remove this section.

#### Example DoCommand

```json
{
  "command_name": {
    "arg1": "foo",
    "arg2": 1
  }
}
```
