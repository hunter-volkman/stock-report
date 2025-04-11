import asyncio
import datetime
import logging
import re
import pytz
import numpy as np
from typing import List, Dict, Optional
from viam.app.viam_client import ViamClient, DataClient
from viam.rpc.dial import DialOptions, Credentials
from openpyxl import Workbook

LOGGER = logging.getLogger(__name__)

class DataExporter:
    """Data exporter for retrieving and processing Viam API data into Excel workbooks."""

    def __init__(self, api_key_id: str, api_key: str, org_id: str, location_id: str, timezone: str = "America/New_York"):
        """Initialize the data exporter.

        Args:
            api_key_id: Viam API key ID
            api_key: Viam API key
            org_id: Viam organization ID
            location_id: Viam location ID
            timezone: Timezone for timestamps (default: America/New_York)
        """
        self.api_key_id = api_key_id
        self.api_key = api_key
        self.org_id = org_id
        self.location_id = location_id
        self.timezone = self._parse_timezone(timezone)
        self.data_client = None

    def _parse_timezone(self, tz_str: str) -> pytz.timezone:
        """Convert a timezone string to a pytz timezone object."""
        try:
            return pytz.timezone(tz_str)
        except Exception as e:
            LOGGER.error(f"Invalid timezone '{tz_str}': {e}")
            return pytz.timezone("America/New_York")

    async def connect(self) -> ViamClient:
        """Connect to the Viam API."""
        LOGGER.info("Connecting to Viam API")
        try:
            dial_options = DialOptions(
                credentials=Credentials(type="api-key", payload=self.api_key),
                auth_entity=self.api_key_id
            )
            client = await ViamClient.create_from_dial_options(dial_options)
            self.data_client = client.data_client
            LOGGER.info("Connected to Viam API")
            return client
        except Exception as e:
            LOGGER.error(f"Failed to connect to Viam API: {e}")
            raise

    async def export_to_excel(self,
                              output_file: str,
                              resource_name: str,
                              start_time: datetime.datetime,
                              end_time: datetime.datetime,
                              bucket_period: str = "PT5M",
                              bucket_method: str = "pct99",
                              include_keys_regex: str = ".*_raw",
                              tab_name: str = "RAW") -> Optional[str]:
        """Export data to an Excel file.

        Args:
            output_file: Path to save the Excel file
            resource_name: Viam resource name to query
            start_time: Start time for the data query
            end_time: End time for the data query
            bucket_period: Time bucket period (ISO8601 duration, default: PT5M)
            bucket_method: Aggregation method (default: pct99)
            include_keys_regex: Regex pattern for keys to include (default: .*_raw)
            tab_name: Name of the worksheet tab (default: RAW)

        Returns:
            Path to the created Excel file or None on failure
        """
        LOGGER.info(f"Exporting data from {start_time} to {end_time} to {output_file}")
        try:
            # Parse bucket_period
            if isinstance(bucket_period, str):
                try:
                    from isodate import parse_duration
                    bucket_period = parse_duration(bucket_period)
                except ImportError:
                    LOGGER.warning("isodate package not available, using default 5-minute bucket")
                    bucket_period = datetime.timedelta(minutes=5)

            # Connect to Viam API
            client = await self.connect()
            try:
                match_predicate = {
                    "organization_id": self.org_id,
                    "component_name": resource_name,
                    "time_received": {"$gte": start_time, "$lt": end_time}
                }
                pipeline = [{"$match": match_predicate}, {"$sort": {"time_received": 1}}]
                all_data = []
                skip, limit = 0, 1000

                while True:
                    LOGGER.info(f"Retrieving data from {skip} to {skip + limit}")
                    batch_pipeline = pipeline.copy()
                    batch_pipeline.extend([{"$skip": skip}, {"$limit": limit}])
                    LOGGER.debug(f"Executing pipeline: {batch_pipeline}")
                    batch = await self.data_client.tabular_data_by_mql(organization_id=self.org_id, query=batch_pipeline)
                    batch_len = len(batch)
                    if batch_len == 0:
                        break
                    all_data.extend(batch)
                    LOGGER.info(f"Retrieved {batch_len} records")
                    if batch_len < limit:
                        break
                    skip += limit

                # Bucket data if needed
                if bucket_period:
                    all_data = self._bucket_data(all_data, bucket_period, bucket_method, include_keys_regex)

                # Create Excel workbook
                wb = Workbook()
                ws = wb.active
                ws.title = tab_name

                if all_data:
                    data_keys = sorted(all_data[0]["data"]["readings"].keys())
                    headers = ["time_received"] + data_keys
                    for col_idx, header in enumerate(headers, 1):
                        ws.cell(row=1, column=col_idx, value=header)

                    for row_idx, row in enumerate(all_data, 2):
                        time_received = row["time_received"].replace(tzinfo=pytz.utc).astimezone(self.timezone)
                        ws.cell(row=row_idx, column=1, value=time_received.replace(tzinfo=None))
                        for col_idx, key in enumerate(data_keys, 2):
                            try:
                                if key in row["data"]["readings"]:
                                    ws.cell(row=row_idx, column=col_idx, value=row["data"]["readings"][key])
                            except Exception as e:
                                LOGGER.warning(f"Error writing value for key {key}: {e}")

                wb.save(output_file)
                LOGGER.info(f"Saved workbook to {output_file} with {len(all_data)} rows")
                return output_file

            finally:
                client.close()

        except Exception as e:
            LOGGER.error(f"Failed to export to Excel: {e}")
            return None

    def _floor_timestamp(self, ts: datetime.datetime, bucket_td: datetime.timedelta) -> datetime.datetime:
        """Round a timestamp down to the nearest bucket interval."""
        epoch = datetime.datetime(1970, 1, 1, tzinfo=ts.tzinfo)
        bucket_count = (ts - epoch) // bucket_td
        return epoch + bucket_count * bucket_td

    def _bucket_data(self, data: List[Dict], bucket_period: datetime.timedelta, bucket_method: str, include_keys_regex: Optional[str] = None) -> List[Dict]:
        """Bucket data by time period and aggregation method.

        Args:
            data: List of data points
            bucket_period: Timedelta object specifying bucket size
            bucket_method: Aggregation method (min, max, avg, first, last, pct95, pct99)
            include_keys_regex: Regex pattern for keys to include

        Returns:
            List of aggregated data points
        """
        LOGGER.info(f"Bucketing data with period {bucket_period} using method {bucket_method}")
        include_regex = re.compile(include_keys_regex) if include_keys_regex else None
        bucketed_data = {}

        for row in data:
            time_received = row["time_received"]
            bucket = self._floor_timestamp(time_received, bucket_period)
            if bucket not in bucketed_data:
                bucketed_data[bucket] = {}
            for key, value in row["data"]["readings"].items():
                if include_regex and not include_regex.match(key):
                    continue
                if key not in bucketed_data[bucket]:
                    bucketed_data[bucket][key] = []
                bucketed_data[bucket][key].append(value)

        LOGGER.debug(f"Created {len(bucketed_data)} time buckets")
        aggregated_data = []
        for bucket, readings in bucketed_data.items():
            aggregated_reading = {}
            for key, values in readings.items():
                if bucket_method == "max":
                    aggregated_reading[key] = max(values)
                elif bucket_method == "min":
                    aggregated_reading[key] = min(values)
                elif bucket_method == "avg":
                    aggregated_reading[key] = sum(values) / len(values)
                elif bucket_method == "first":
                    aggregated_reading[key] = values[0]
                elif bucket_method == "last":
                    aggregated_reading[key] = values[-1]
                elif bucket_method == "pct95":
                    aggregated_reading[key] = np.percentile(values, 95)
                elif bucket_method == "pct99":
                    aggregated_reading[key] = np.percentile(values, 99)
                else:
                    LOGGER.warning(f"Unsupported bucket method: {bucket_method}, using max")
                    aggregated_reading[key] = max(values)
            aggregated_data.append({"time_received": bucket, "data": {"readings": aggregated_reading}})

        aggregated_data.sort(key=lambda x: x["time_received"])
        return aggregated_data