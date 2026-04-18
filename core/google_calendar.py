import logging
import os
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone

log = logging.getLogger("red.luma.google")


class GoogleCalendarClient:
    """Google Calendar API client for managing aggregate calendars."""

    def __init__(self, credentials_data: Optional[Dict] = None):
        self.credentials_data = credentials_data
        self._service = None

    async def _get_service(self):
        """Get authenticated Google Calendar service."""
        if self._service:
            return self._service

        try:
            from google.oauth2 import service_account
            from googleapiclient.discovery import build

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
                scopes=['https://www.googleapis.com/auth/calendar.calendarlist']
            )

            self._service = build('calendar', 'v3', credentials=credentials)
            return self._service

        except ImportError:
            raise ImportError(
                "Google Calendar API libraries not installed. "
                "Run: pip install google-api-python-client google-auth"
            )

    async def add_calendar_to_list(self, ics_url: str, calendar_name: str) -> Dict[str, Any]:
        """Add an external ICS calendar to the authenticated user's calendar list.

        Args:
            ics_url: The ICS feed URL to add
            calendar_name: Friendly name for the calendar

        Returns:
            Dict with success status and calendar info
        """
        try:
            service = await self._get_service()

            calendar_list_entry = {
                'id': ics_url,
                'summaryOverride': calendar_name,
                'selected': True,
            }

            result = service.calendarList().insert(
                body=calendar_list_entry,
                colorRgbFormat=True
            ).execute()

            log.info(f"Added calendar to list: {calendar_name} ({ics_url})")
            return {
                'success': True,
                'calendar_id': result.get('id'),
                'summary': result.get('summaryOverride', calendar_name),
            }

        except Exception as e:
            if 'already exists' in str(e).lower() or 'duplicate' in str(e).lower():
                log.info(f"Calendar already in list: {calendar_name}")
                return {
                    'success': True,
                    'calendar_id': ics_url,
                    'summary': calendar_name,
                    'already_exists': True,
                }
            log.error(f"Error adding calendar to list: {e}")
            return {
                'success': False,
                'error': str(e),
            }

    async def remove_calendar_from_list(self, ics_url: str) -> Dict[str, Any]:
        """Remove an external calendar from the calendar list.

        Args:
            ics_url: The ICS feed URL to remove

        Returns:
            Dict with success status
        """
        try:
            service = await self._get_service()

            service.calendarList().delete(calendarId=ics_url).execute()

            log.info(f"Removed calendar from list: {ics_url}")
            return {'success': True}

        except Exception as e:
            if 'not found' in str(e).lower():
                return {'success': True, 'not_found': True}
            log.error(f"Error removing calendar from list: {e}")
            return {'success': False, 'error': str(e)}

    async def list_calendars(self) -> List[Dict[str, Any]]:
        """List all calendars in the calendar list.

        Returns:
            List of calendar info dicts
        """
        try:
            service = await self._get_service()

            result = service.calendarList().list().execute()
            calendars = result.get('items', [])

            return [
                {
                    'id': cal.get('id'),
                    'summary': cal.get('summary'),
                    'summary_override': cal.get('summaryOverride'),
                    'selected': cal.get('selected', False),
                    'access_role': cal.get('accessRole'),
                }
                for cal in calendars
            ]

        except Exception as e:
            log.error(f"Error listing calendars: {e}")
            return []

    async def get_calendar_info(self, calendar_id: str) -> Optional[Dict[str, Any]]:
        """Get info about a specific calendar.

        Args:
            calendar_id: The calendar ID to look up

        Returns:
            Calendar info dict or None
        """
        try:
            service = await self._get_service()

            result = service.calendarList().get(calendarId=calendar_id).execute()

            return {
                'id': result.get('id'),
                'summary': result.get('summary'),
                'summary_override': result.get('summaryOverride'),
                'selected': result.get('selected', False),
                'access_role': result.get('accessRole'),
                'time_zone': result.get('timeZone'),
            }

        except Exception as e:
            log.error(f"Error getting calendar info: {e}")
            return None

    async def test_connection(self) -> Dict[str, Any]:
        """Test the Google Calendar API connection.

        Returns:
            Dict with connection status and service account info
        """
        try:
            service = await self._get_service()

            result = service.calendarList().list(maxResults=1).execute()

            return {
                'success': True,
                'service_account': self.credentials_data.get('client_email') if self.credentials_data else 'env',
                'calendars_accessible': len(result.get('items', [])),
            }

        except Exception as e:
            return {
                'success': False,
                'error': str(e),
            }
