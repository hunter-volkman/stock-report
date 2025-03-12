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
  "recipients": "<string or array of strings>",
  "location": "<string>",
  "api_key_id": "<string>",
  "api_key": "<string>",
  "org_id": "<string>",
  "process_time": "<string>",
  "send_time": "<string>",
  "save_dir": "<string>",
  "export_script": "<string>"
}
```

#### Attributes

The following attributes are available for this model:

| Name          | Type   | Inclusion | Description                |
|---------------|--------|-----------|----------------------------|
| `email` | string  | Required  | GMail address for sending emails. |
| `password` | string | Required  | Description of attribute 2 |

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
