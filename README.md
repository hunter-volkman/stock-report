# Module stock-report

This module generates and emails Excel report workbooks based on sensor data from Viam's API on a scheduled basis. It also captures and attaches images throughout the day alongside the Excel report. The module has robust scheduling and state management to ensure reliable operation even across restarts.

## Model `hunter:stock-report:email`

A sensor component that exports data from the Viam API, processes Excel workbooks using templates, captures images at scheduled intervals, and emails comprehensive reports on a configurable schedule.

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
  "include_images": true,
  "camera_name": "remote-1:ffmpeg",
  "capture_times": ["08:00", "10:00", "12:00", "14:00", "16:00", "18:00"],
  "image_width": 640,
  "image_height": 480,
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

| `include_images` | string | Optional | Whether to include images in the report (default: false). |
| `camera_name` | Required if include_images | Optional | Name of the camera component to capture images. |
| `capture_times` | list[str] | Optional | Times to capture images (HH format, default: ["08:00", "10:00", "12:00", "14:00", "16:00", "18:00"]). |
| `image_width` | int | Optional | Width of captured images in pixels (default: 640). |
| `image_height` | int | Optional | Height of captured images in pixels (default: 480). |

| `hours_mon` to `hours_sun` | list[str] | Store hours [open, close] for each day. |

#### Example Configuration

```json
{
    "location": "New York Store",
    "recipients": ["reports@example.com", "manager@example.com"],
    "api_key_id": "12345abcde",
    "api_key": "your-viam-api-key",
    "org_id": "org12345",
    "sendgrid_api_key": "SG.your-sendgrid-key",
    "filename_prefix": "nyc_store",
    "teleop_url": "https://app.viam.com/robots/12345/part/togo?camera=ffmpeg",
    "sender_email": "reports@example.com",
    "sender_name": "Store Reports",
    "send_time": "20:00",
    "process_time": "19:00",
    "timezone": "America/New_York",
    "include_images": true,
    "camera_name": "remote-1:ffmpeg",
    "capture_times": ["08:00", "10:00", "12:00", "14:00", "16:00", "18:00"],
    "image_width": 640,
    "image_height": 480,
    "hours_mon": ["07:00", "19:30"],
    "hours_tue": ["07:00", "19:30"],
    "hours_wed": ["07:00", "19:30"],
    "hours_thu": ["07:00", "19:30"],
    "hours_fri": ["07:00", "19:30"],
    "hours_sat": ["08:00", "17:00"],
    "hours_sun": ["08:00", "17:00"]
  }
```

### DoCommand
