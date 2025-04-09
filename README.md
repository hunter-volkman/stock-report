# Module stock-report

This module generates and emails Excel report workbooks based on sensor data from Viam's API on a scheduled basis. It fetches data for specific time periods, processes it using a template, and emails the resulting reports to configured recipients.

## Model `hunter:stock-report:email`

A sensor component that exports data from the Viam API, processes Excel workbooks using templates, and emails reports on a configurable schedule.

### Configuration
Configure the model using the following JSON template in your Viam robot configuration:

```json
{
  "location": "Store Location Name",
  "recipients": ["user@example.com", "manager@example.com"],
  "api_key_id": "your-viam-api-key-id",
  "api_key": "your-viam-api-key",
  "org_id": "your-viam-org-id",
  "sendgrid_api_key": "your-sendgrid-api-key",
  "filename_prefix": "store_name_city",
  "sender_email": "reports@example.com",
  "sender_name": "Store Reports",
  "send_time": "20:00",
  "process_time": "19:00",
  "timezone": "America/New_York",
  "hours_mon": ["07:00", "19:30"],
  "hours_tue": ["07:00", "19:30"],
  "hours_wed": ["07:00", "19:30"],
  "hours_thu": ["07:00", "19:30"],
  "hours_fri": ["07:00", "19:30"],
  "hours_sat": ["08:00", "17:00"],
  "hours_sun": ["08:00", "17:00"]
}
```

#### Attributes

The following attributes are available for this model:

| Name          | Type   | Inclusion | Description                |
|---------------|--------|-----------|----------------------------|
| `location` | string | Required | Location name for reports and emails. |
| `recipients` | list[str] | Required | Email recipients for reports. |
| `api_key_id` | string | Required | Email addresses to receive the daily report. |
| `api_key` | string | Required | Viam API key. |
| `api_key_id` | string | Required | Viam API key ID. |
| `org_id` | string | Required | Viam organization ID. |
| `sendgrid_api_key` | string | Required | SendGrid API key for sending emails. |
| `filename_prefix` | string | Optional | Prefix for output filenames (e.g., "store_name_city"). |
| `teleop_url` | string | Optional | URL to the teleop interface for real-time store view. |
| `sender_email` | string | Optional | Email address for the sender (default: "no-reply@viam.com"). |
| `sender_name` | string | Optional | Display name for the sender (default: "Stock Report Module") |
| `send_time` | string | Optional | Time to send reports (HH format, default: "20:00"). |
| `process_time` | string | Optional | Time to process workbooks (HH format, default: 1 hour before send_time). |
| `timezone` | string | Optional | Timezone for scheduling (default: "America/New_York"). |
| `hours_mon` to `hours_sun` | list[str] | Store hours [open, close] for each day. |

#### Example Configuration

```json
{
  
}
```

### DoCommand
