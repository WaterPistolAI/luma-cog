import sqlite3
import logging
import asyncio
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Any, Set
from pathlib import Path

log = logging.getLogger("red.luma.database")


class EventDatabase:
    """
    SQLite database for tracking Luma events to detect changes and avoid duplicates.

    Database schema:
    - events: Store event metadata for change detection
    - event_history: Track when events were sent to Discord
    """

    def __init__(self, cog_instance=None, db_path: Optional[str] = None):
        """
        Initialize the event database.

        Args:
            cog_instance: The cog instance to get the data directory from
            db_path: Optional custom database path. If None, uses default location.
        """
        if db_path:
            self.db_path = db_path
        elif cog_instance:
            # Try to use RedBot's data directory if available
            try:
                from redbot.core import Config

                # Use the cog's data directory through Config
                data_dir = (
                    Path(Config.get_conf(cog_instance).get_base_config().data_dir)
                    / "luma"
                )
                data_dir.mkdir(parents=True, exist_ok=True)
                self.db_path = str(data_dir / "luma_events.db")
            except (ImportError, AttributeError, Exception):
                # Fallback to a safe default location
                self._setup_fallback_database_path()
        else:
            # Fallback to a safe default location
            self._setup_fallback_database_path()

        self._lock = asyncio.Lock()
        self._initialize_database()

    def _setup_fallback_database_path(self):
        """Set up a fallback database path that should work in most environments."""
        try:
            # Try to use the user's home directory
            from pathlib import Path
            import os

            # Try to create a luma directory in a writable location
            possible_paths = [
                Path.cwd() / "data" / "luma_events.db",
                Path.home() / ".luma" / "luma_events.db",
                Path("/tmp") / "luma_events.db",
            ]

            for path in possible_paths:
                try:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    # Test write access
                    test_file = path.parent / "test_write.tmp"
                    test_file.write_text("test")
                    test_file.unlink()
                    self.db_path = str(path)
                    log.info(f"Using database path: {self.db_path}")
                    return
                except (PermissionError, OSError):
                    continue

            # If all else fails, use memory database
            self.db_path = ":memory:"
            log.warning("Using in-memory database as fallback")

        except Exception as e:
            # Absolute fallback
            self.db_path = ":memory:"
            log.error(f"Database path setup failed: {e}. Using in-memory database")

    def _initialize_database(self):
        """Initialize the database and create tables if they don't exist."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Create events table for tracking unique events
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS events (
                        event_api_id TEXT PRIMARY KEY,
                        calendar_api_id TEXT NOT NULL,
                        name TEXT NOT NULL,
                        start_at TEXT NOT NULL,
                        end_at TEXT,
                        timezone TEXT,
                        event_type TEXT,
                        url TEXT,
                        last_modified TEXT,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                """
                )

                # Create event_history table for tracking sent messages
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS event_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        event_api_id TEXT NOT NULL,
                        guild_id INTEGER NOT NULL,
                        channel_id INTEGER NOT NULL,
                        message_id INTEGER,
                        start_at TEXT,
                        sent_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (event_api_id) REFERENCES events (event_api_id)
                    )
                """
                )

                # Create indexes for performance
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_events_calendar 
                    ON events(calendar_api_id)
                """
                )

                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_events_start_time 
                    ON events(start_at)
                """
                )

                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_event_history_guild 
                    ON event_history(guild_id, channel_id)
                """
                )

                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_event_history_event_guild
                    ON event_history(event_api_id, guild_id)
                """
                )

                conn.commit()

                # Migrate: add message_id and start_at columns if missing
                try:
                    cursor.execute("SELECT message_id FROM event_history LIMIT 1")
                except sqlite3.OperationalError:
                    cursor.execute("ALTER TABLE event_history ADD COLUMN message_id INTEGER")
                    log.info("Added message_id column to event_history")

                try:
                    cursor.execute("SELECT start_at FROM event_history LIMIT 1")
                except sqlite3.OperationalError:
                    cursor.execute("ALTER TABLE event_history ADD COLUMN start_at TEXT")
                    log.info("Added start_at column to event_history")

                conn.commit()
                log.info("Event database initialized successfully")

        except Exception as e:
            log.error(f"Failed to initialize database: {e}")
            raise

    async def get_tracked_events(self, calendar_api_id: str) -> List[Dict[str, Any]]:
        """Get all tracked events for a specific calendar."""
        async with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()

                    cursor.execute(
                        """
                        SELECT * FROM events 
                        WHERE calendar_api_id = ? 
                        ORDER BY start_at
                    """,
                        (calendar_api_id,),
                    )

                    return [dict(row) for row in cursor.fetchall()]

            except Exception as e:
                log.error(f"Failed to get tracked events for {calendar_api_id}: {e}")
                return []

    async def upsert_events(
        self, events: List[Dict[str, Any]], calendar_api_id: str
    ) -> Dict[str, Any]:
        """
        Update or insert events and return information about changes.

        Returns:
            Dict with 'new_events', 'updated_events', 'deleted_events' counts,
            and 'new_event_data' list containing the actual new event data
        """
        async with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()

                    cursor.execute(
                        """
                        SELECT event_api_id, last_modified FROM events
                        WHERE calendar_api_id = ?
                    """,
                        (calendar_api_id,),
                    )
                    existing_events = {row[0]: row[1] for row in cursor.fetchall()}

                    new_events_count = 0
                    updated_events = 0
                    new_event_data = []
                    current_time = datetime.now(timezone.utc).isoformat()

                    for event_data in events:
                        event_api_id = event_data["api_id"]

                        try:
                            cursor.execute(
                                """
                                INSERT OR REPLACE INTO events (
                                    event_api_id, calendar_api_id, name, start_at, end_at,
                                    timezone, event_type, url, last_modified, created_at, updated_at
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                                (
                                    event_api_id,
                                    calendar_api_id,
                                    event_data.get("name", ""),
                                    event_data.get("start_at", ""),
                                    event_data.get("end_at"),
                                    event_data.get("timezone"),
                                    event_data.get("event_type"),
                                    event_data.get("url"),
                                    event_data.get("last_modified", current_time),
                                    current_time,
                                    current_time,
                                ),
                            )

                            if event_api_id in existing_events:
                                updated_events += 1
                            else:
                                new_events_count += 1
                                new_event_data.append(event_data)

                        except Exception as e:
                            log.warning(f"Failed to upsert event {event_api_id}: {e}")
                            continue

                    current_event_ids = {event["api_id"] for event in events}
                    deleted_events = len(existing_events) - len(
                        current_event_ids.intersection(existing_events.keys())
                    )

                    if deleted_events > 0:
                        deleted_event_ids = list(
                            set(existing_events.keys()) - current_event_ids
                        )
                        placeholders = ",".join(["?" for _ in deleted_event_ids])
                        cursor.execute(
                            f"""
                            DELETE FROM events 
                            WHERE event_api_id IN ({placeholders})
                        """,
                            deleted_event_ids,
                        )

                    conn.commit()

                    log.info(
                        f"Event sync for {calendar_api_id}: {new_events_count} new, {updated_events} updated, {deleted_events} deleted"
                    )

                    return {
                        "new_events": new_events_count,
                        "updated_events": updated_events,
                        "deleted_events": deleted_events,
                        "total_events": len(events),
                        "new_event_data": new_event_data,
                    }

            except Exception as e:
                log.error(f"Failed to upsert events for {calendar_api_id}: {e}")
                return {
                    "new_events": 0,
                    "updated_events": 0,
                    "deleted_events": 0,
                    "total_events": 0,
                    "new_event_data": [],
                }

    async def get_new_events(
        self, calendar_api_id: str, events: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Get only the new events that haven't been tracked before."""
        async with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()

                    cursor.execute(
                        """
                        SELECT event_api_id FROM events 
                        WHERE calendar_api_id = ?
                    """,
                        (calendar_api_id,),
                    )

                    existing_event_ids = {row[0] for row in cursor.fetchall()}

                    new_events = [
                        event
                        for event in events
                        if event["api_id"] not in existing_event_ids
                    ]

                    return new_events

            except Exception as e:
                log.error(f"Failed to get new events for {calendar_api_id}: {e}")
                return events  # Return all events if database query fails

    async def record_event_sent(
        self, event_api_id: str, guild_id: int, channel_id: int,
        message_id: int = None, start_at: str = None
    ):
        """Record that an event was sent to a Discord channel."""
        async with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()

                    cursor.execute(
                        """
                        INSERT INTO event_history (event_api_id, guild_id, channel_id, message_id, start_at)
                        VALUES (?, ?, ?, ?, ?)
                    """,
                        (event_api_id, guild_id, channel_id, message_id, start_at),
                    )

                    conn.commit()
                    log.debug(
                        f"Recorded event {event_api_id} sent to guild {guild_id}, "
                        f"channel {channel_id}, message {message_id}"
                    )

            except Exception as e:
                log.error(f"Failed to record event sent: {e}")

    async def was_event_recently_sent(
        self, event_api_id: str, guild_id: int, hours: int = 24
    ) -> bool:
        async with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()

                    cutoff = (
                        datetime.now(timezone.utc) - timedelta(hours=hours)
                    ).isoformat()

                    cursor.execute(
                        """
                        SELECT COUNT(*) FROM event_history 
                        WHERE event_api_id = ? AND guild_id = ? AND sent_at >= ?
                    """,
                        (event_api_id, guild_id, cutoff),
                    )

                    count = cursor.fetchone()[0]
                    return count > 0

            except Exception as e:
                log.error(f"Failed to check recent event send: {e}")
                return False

    async def get_sent_event_ids_for_guild(
        self, guild_id: int, hours: int = 48
    ) -> Set[str]:
        async with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()

                    cutoff = (
                        datetime.now(timezone.utc) - timedelta(hours=hours)
                    ).isoformat()

                    cursor.execute(
                        """
                        SELECT DISTINCT event_api_id FROM event_history
                        WHERE guild_id = ? AND sent_at >= ?
                    """,
                        (guild_id, cutoff),
                    )

                    return {row[0] for row in cursor.fetchall()}

            except Exception as e:
                log.error(f"Failed to get sent event IDs for guild {guild_id}: {e}")
                return set()

    async def get_expired_messages(self, hours_after_event: int = 2) -> List[Dict[str, Any]]:
        """Get messages for events that have already passed.

        Args:
            hours_after_event: Hours after event end time before marking as expired

        Returns:
            List of dicts with guild_id, channel_id, message_id, event_api_id
        """
        async with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()

                    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_after_event)

                    cursor.execute(
                        """
                        SELECT eh.guild_id, eh.channel_id, eh.message_id, eh.event_api_id,
                               eh.id as history_id, e.start_at, e.end_at, e.name
                        FROM event_history eh
                        JOIN events e ON eh.event_api_id = e.event_api_id
                        WHERE eh.message_id IS NOT NULL
                        AND e.start_at < ?
                    """,
                        (cutoff.isoformat(),),
                    )

                    results = []
                    for row in cursor.fetchall():
                        results.append({
                            'guild_id': row['guild_id'],
                            'channel_id': row['channel_id'],
                            'message_id': row['message_id'],
                            'event_api_id': row['event_api_id'],
                            'history_id': row['history_id'],
                            'event_name': row['name'],
                        })

                    return results

            except Exception as e:
                log.error(f"Failed to get expired messages: {e}")
                return []

    async def delete_history_records(self, history_ids: List[int]):
        """Delete event history records by their IDs."""
        if not history_ids:
            return

        async with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    placeholders = ",".join(["?" for _ in history_ids])
                    cursor.execute(
                        f"DELETE FROM event_history WHERE id IN ({placeholders})",
                        history_ids,
                    )
                    conn.commit()
                    log.debug(f"Deleted {len(history_ids)} history records")
            except Exception as e:
                log.error(f"Failed to delete history records: {e}")

    async def get_calendar_stats(self) -> Dict[str, Any]:
        """Get statistics about tracked calendars and events."""
        async with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()

                    # Get total counts
                    cursor.execute("SELECT COUNT(*) FROM events")
                    total_events = cursor.fetchone()[0]

                    cursor.execute("SELECT COUNT(DISTINCT calendar_api_id) FROM events")
                    total_calendars = cursor.fetchone()[0]

                    cursor.execute("SELECT COUNT(*) FROM event_history")
                    total_sends = cursor.fetchone()[0]

                    # Get calendar-specific stats
                    cursor.execute(
                        """
                        SELECT calendar_api_id, COUNT(*) as event_count 
                        FROM events 
                        GROUP BY calendar_api_id 
                        ORDER BY event_count DESC
                    """
                    )

                    calendar_stats = [
                        {"calendar_api_id": row[0], "event_count": row[1]}
                        for row in cursor.fetchall()
                    ]

                    return {
                        "total_events": total_events,
                        "total_calendars": total_calendars,
                        "total_sends": total_sends,
                        "calendar_stats": calendar_stats,
                    }

            except Exception as e:
                log.error(f"Failed to get calendar stats: {e}")
                return {}

    async def cleanup_old_history(self, days: int = 30):
        """Clean up old event history records to prevent database bloat."""
        async with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()

                    cutoff_date = (
                        datetime.now(timezone.utc)
                        .replace(day=datetime.now(timezone.utc).day - days)
                        .isoformat()
                    )

                    cursor.execute(
                        """
                        DELETE FROM event_history
                        WHERE sent_at < ?
                    """,
                        (cutoff_date,),
                    )

                    deleted_count = cursor.rowcount
                    conn.commit()

                    log.info(f"Cleaned up {deleted_count} old event history records")
                    return deleted_count

            except Exception as e:
                log.error(f"Failed to cleanup old history: {e}")
                return 0

    async def clear_event_database(
        self, calendar_api_ids: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Clear event tracking data from the database.

        This method clears events and event history data while preserving
        the database structure. Can clear all data or just specific calendars.

        Args:
            calendar_api_ids: Optional list of calendar API IDs to clear.
                            If None, clears all event data.

        Returns:
            Dict with counts of cleared records and success status
        """
        async with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()

                    if calendar_api_ids is None:
                        # Clear all event data (global clear)
                        log.info("Starting global event database clear")

                        # Get counts before clearing
                        cursor.execute("SELECT COUNT(*) FROM events")
                        events_count = cursor.fetchone()[0]

                        cursor.execute("SELECT COUNT(*) FROM event_history")
                        history_count = cursor.fetchone()[0]

                        # Clear all event data (preserves database structure)
                        cursor.execute("DELETE FROM events")
                        cursor.execute("DELETE FROM event_history")

                        conn.commit()

                        log.info(
                            f"Global event database cleared: {events_count} events, {history_count} history records"
                        )

                        return {
                            "events_cleared": events_count,
                            "history_cleared": history_count,
                            "success": True,
                            "type": "global",
                        }
                    else:
                        # Clear specific calendar(s) only
                        log.info(
                            f"Starting group-specific database clear for calendars: {calendar_api_ids}"
                        )

                        # Build placeholders for the IN clause
                        placeholders = ",".join(["?" for _ in calendar_api_ids])

                        # Get counts before clearing
                        cursor.execute(
                            f"SELECT COUNT(*) FROM events WHERE calendar_api_id IN ({placeholders})",
                            calendar_api_ids,
                        )
                        events_count = cursor.fetchone()[0]

                        # Get history records that would be affected
                        cursor.execute(
                            """SELECT COUNT(*) FROM event_history eh
                               JOIN events e ON eh.event_api_id = e.event_api_id
                               WHERE e.calendar_api_id IN ({})""".format(
                                placeholders
                            ),
                            calendar_api_ids,
                        )
                        history_count = cursor.fetchone()[0]

                        # Clear event data for specific calendars
                        cursor.execute(
                            f"DELETE FROM events WHERE calendar_api_id IN ({placeholders})",
                            calendar_api_ids,
                        )

                        # Clear history records for events from these calendars
                        cursor.execute(
                            """DELETE FROM event_history
                               WHERE event_api_id IN (
                                   SELECT event_api_id FROM events WHERE calendar_api_id IN ({})
                               )""".format(
                                placeholders
                            ),
                            calendar_api_ids,
                        )

                        conn.commit()

                        log.info(
                            f"Group-specific database clear completed: {events_count} events, {history_count} history records for calendars {calendar_api_ids}"
                        )

                        return {
                            "events_cleared": events_count,
                            "history_cleared": history_count,
                            "success": True,
                            "type": "group_specific",
                            "calendars_cleared": calendar_api_ids,
                        }

            except Exception as e:
                log.error(f"Failed to clear event database: {e}")
                return {
                    "events_cleared": 0,
                    "history_cleared": 0,
                    "success": False,
                    "error": str(e),
                    "type": calendar_api_ids and "group_specific" or "global",
                }

    async def get_calendars_for_group(
        self, group_name: str, guild_channel_groups: Dict
    ) -> List[str]:
        """Get calendar API IDs for a specific group.

        Args:
            group_name: Name of the channel group
            guild_channel_groups: Dictionary of guild channel groups

        Returns:
            List of calendar API IDs for the group, or empty list if group not found
        """
        if group_name not in guild_channel_groups:
            log.warning(f"Group '{group_name}' not found in guild configuration")
            return []

        try:
            from ..models.data_models import ChannelGroup

            group = ChannelGroup.from_dict(guild_channel_groups[group_name])
            log.debug(
                f"Found group '{group_name}' with {len(group.subscription_ids)} subscriptions"
            )
            return group.subscription_ids
        except Exception as e:
            log.error(f"Failed to get calendars for group '{group_name}': {e}")
            return []
