# Module workbook-emailer 

This module enables a Raspberry Pi to autonomously fetch sensor data, process Excel workbooks, and email daily reports with updated data. It connects to a Viam machine's data export API to retrieve langer fill level data, updates a master workbook with new data, and emails the results. The module persists its state to resume after power cycles or restarts.

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
