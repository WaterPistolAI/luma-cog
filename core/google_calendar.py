import logging
import os
import asyncio
import aiohttp
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

log = logging.getLogger("red.luma.google")


class GoogleCalendarClient:
    """Google Calendar API client for managing aggregate calendars.

    Creates events directly in a target Google Calendar by fetching
    event data from Luma and pushing them via the Calendar API.
    """

    SCOPES = [
        'https://www.googleapis.com/auth/calendar.events',
        'https://www.googleapis.com/auth/calendar.calendarlist.readonly',
    ]

    def __init__(self, credentials_data: Optional[Dict] = None):
        self.credentials_data = credentials_data
        self._service = None

    async def _get_service(self):
        if self._service:
            return self._service

        try:
            from google.oauth2 import service_account
            from googleapiclient.discovery import build
        except ImportError as e:
            raise ImportError(
                "Google Calendar API libraries not installed. "
                "Run: pip install google-api-python-client google-auth"
            ) from e

        try:
            if not self.credentials_data:
                creds_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
                if creds_path and os.path.exists(creds_path):
                    import json
                    with open(creds_path, 'r') as f:
                        self.credentials_data = json.load(f)
                else:
                    raise ValueError("No Google credentials configured")

            credentials = service_account.Credentials.from_service_account_info(
                self.credentials_data,
                scopes=self.SCOPES,
            )

            self._service = build('calendar', 'v3', credentials=credentials)
            return self._service

        except Exception as e:
            if isinstance(e, ValueError):
                raise
            log.error(f"Google Calendar connection error: {e}", exc_info=True)
            raise

    async def test_connection(self) -> Dict[str, Any]:
        try:
            service = await self._get_service()
            result = service.calendarList().list(maxResults=1).execute()
            return {
                'success': True,
                'service_account': self.credentials_data.get('client_email') if self.credentials_data else 'env',
                'calendars_accessible': len(result.get('items', [])),
            }
        except Exception as e:
            return {'success': False, 'error': str(e)}

    async def list_calendars(self) -> List[Dict[str, Any]]:
        try:
            service = await self._get_service()
            result = service.calendarList().list().execute()
            return [
                {
                    'id': cal.get('id'),
                    'summary': cal.get('summary'),
                    'access_role': cal.get('accessRole'),
                }
                for cal in result.get('items', [])
            ]
        except Exception as e:
            log.error(f"Error listing calendars: {e}")
            return []

    def _event_to_google(self, event) -> Dict[str, Any]:
        start_at = datetime.fromisoformat(event.start_at.replace("Z", "+00:00"))
        end_at_str = event.end_at
        if end_at_str:
            end_at = datetime.fromisoformat(end_at_str.replace("Z", "+00:00"))
        else:
            end_at = start_at

        tz = event.timezone or "UTC"
        tz_info = ZoneInfo(tz)

        local_start = start_at.astimezone(tz_info)
        local_end = end_at.astimezone(tz_info)

        google_event = {
            'summary': event.name,
            'start': {
                'dateTime': local_start.strftime('%Y-%m-%dT%H:%M:%S'),
                'timeZone': tz,
            },
            'end': {
                'dateTime': local_end.strftime('%Y-%m-%dT%H:%M:%S'),
                'timeZone': tz,
            },
            'source': {
                'title': 'Luma',
                'url': f"https://lu.ma/{event.url}" if event.url else "https://lu.ma",
            },
        }

        description_parts = []
        if hasattr(event, 'calendar') and event.calendar:
            cal_name = getattr(event.calendar, 'name', '')
            cal_slug = getattr(event.calendar, 'slug', '')
            if cal_name:
                description_parts.append(f"Calendar: {cal_name}")
            if cal_slug:
                description_parts.append(f"https://lu.ma/{cal_slug}")

        event_url = f"https://lu.ma/{event.url}" if event.url else ""
        if event_url:
            description_parts.append(f"Event: {event_url}")

        if description_parts:
            google_event['description'] = '\n'.join(description_parts)

        return google_event

    async def create_event(self, calendar_id: str, event) -> Dict[str, Any]:
        """Create a single event in the target Google Calendar.

        Args:
            calendar_id: The Google Calendar ID
            event: A Luma Event object

        Returns:
            Dict with success status and Google event ID
        """
        try:
            service = await self._get_service()
            body = self._event_to_google(event)

            result = service.events().insert(
                calendarId=calendar_id,
                body=body,
            ).execute()

            log.info(f"Created Google event: {event.name} -> {result.get('id')}")
            return {
                'success': True,
                'google_event_id': result.get('id'),
                'luma_event_api_id': event.api_id,
                'html_link': result.get('htmlLink'),
            }

        except Exception as e:
            log.error(f"Error creating event {event.name}: {e}")
            return {
                'success': False,
                'error': str(e),
                'luma_event_api_id': event.api_id,
            }

    async def sync_events(
        self,
        calendar_id: str,
        events: list,
        existing_mapping: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Sync Luma events to the target Google Calendar.

        Creates new events and updates existing ones. Tracks mapping
        between Luma event IDs and Google event IDs.

        Args:
            calendar_id: The Google Calendar ID
            events: List of Luma Event objects
            existing_mapping: Dict of luma_api_id -> google_event_id

        Returns:
            Dict with sync results and updated mapping
        """
        mapping = dict(existing_mapping) if existing_mapping else {}
        stats = {'created': 0, 'updated': 0, 'skipped': 0, 'failed': 0, 'errors': []}

        service = await self._get_service()

        for event in events:
            luma_id = event.api_id

            try:
                body = self._event_to_google(event)

                if luma_id in mapping:
                    google_id = mapping[luma_id]
                    service.events().update(
                        calendarId=calendar_id,
                        eventId=google_id,
                        body=body,
                    ).execute()
                    stats['updated'] += 1
                    log.debug(f"Updated Google event: {event.name}")
                else:
                    result = service.events().insert(
                        calendarId=calendar_id,
                        body=body,
                    ).execute()
                    mapping[luma_id] = result.get('id')
                    stats['created'] += 1
                    log.debug(f"Created Google event: {event.name}")

                await asyncio.sleep(0.1)

            except Exception as e:
                stats['failed'] += 1
                stats['errors'].append(f"{event.name}: {str(e)}")
                log.error(f"Error syncing event {event.name}: {e}")

        return {
            'success': True,
            'stats': stats,
            'mapping': mapping,
        }

    async def delete_event(self, calendar_id: str, google_event_id: str) -> Dict[str, Any]:
        """Delete an event from the target Google Calendar."""
        try:
            service = await self._get_service()
            service.events().delete(
                calendarId=calendar_id,
                eventId=google_event_id,
            ).execute()
            return {'success': True}
        except Exception as e:
            if 'not found' in str(e).lower():
                return {'success': True, 'not_found': True}
            log.error(f"Error deleting event: {e}")
            return {'success': False, 'error': str(e)}

    async def clear_calendar(self, calendar_id: str, mapping: Dict[str, str]) -> Dict[str, Any]:
        """Delete all mapped events from the target Google Calendar."""
        deleted = 0
        failed = 0

        for luma_id, google_id in mapping.items():
            result = await self.delete_event(calendar_id, google_id)
            if result.get('success'):
                deleted += 1
            else:
                failed += 1

        return {'deleted': deleted, 'failed': failed}

    async def validate_ics_feed(self, ics_url: str) -> Dict[str, Any]:
        """Validate that an ICS feed is accessible."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.head(ics_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        return {'valid': True, 'status': response.status}
                    else:
                        return {'valid': False, 'status': response.status, 'error': f'HTTP {response.status}'}
        except aiohttp.ClientError as e:
            return {'valid': False, 'error': f'Connection error: {str(e)}'}
        except Exception as e:
            return {'valid': False, 'error': str(e)}
