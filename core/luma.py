import sys
import os
import asyncio
import logging
import urllib.parse
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional, Union
import discord
from redbot.core import Config, commands, checks
from redbot.core.bot import Red
from redbot.core.utils import menus

import pytz

from ..models.calendar_get import Event, FeaturedItem, Host, Calendar
from ..models.data_models import Subscription, ChannelGroup
from .api_client import (
    LumaAPIClient,
    LumaAPIError,
    LumaAPIRateLimitError,
    LumaAPINotFoundError,
)
from .database import EventDatabase


log = logging.getLogger("red.luma")


def parse_calendar_identifier(input_str: str) -> str:
    """Parse calendar ID from various input formats.

    Accepts:
    - Raw API ID: cal-xxxxx
    - Slug: genai-ny
    - ICS URL: https://api2.luma.com/ics/get?entity=calendar&id=cal-xxxxx
    - Google Calendar URL: https://www.google.com/calendar/render?cid=...
    - Outlook URL: https://outlook.live.com/calendar/0/addcalendar?url=...

    Returns:
        Calendar ID (cal-xxxxx) or original input if not a URL
    """
    import re
    from urllib.parse import urlparse, parse_qs, unquote

    input_str = input_str.strip()

    # Already a calendar ID (starts with cal-)
    if input_str.startswith('cal-'):
        return input_str

    # Check if it's a URL
    if not input_str.startswith('http'):
        # Assume it's a slug
        return input_str

    try:
        # Decode URL encoding
        decoded = unquote(input_str)

        # Try to extract cal- ID from the URL
        cal_match = re.search(r'cal-[a-zA-Z0-9]+', decoded)
        if cal_match:
            return cal_match.group(0)

        # Try to extract from query parameters
        parsed = urlparse(decoded)
        params = parse_qs(parsed.query)

        # Check for 'id' parameter
        if 'id' in params:
            return params['id'][0]

        # Check for 'cid' parameter (Google Calendar)
        if 'cid' in params:
            cid = params['cid'][0]
            cid_decoded = unquote(cid)
            cal_match = re.search(r'cal-[a-zA-Z0-9]+', cid_decoded)
            if cal_match:
                return cal_match.group(0)

        # Check for 'url' parameter (Outlook)
        if 'url' in params:
            url_param = params['url'][0]
            url_decoded = unquote(url_param)
            cal_match = re.search(r'cal-[a-zA-Z0-9]+', url_decoded)
            if cal_match:
                return cal_match.group(0)

    except Exception as e:
        log.debug(f"Error parsing URL: {e}")

    # Return original input as fallback
    return input_str


def get_timezone_abbr(timezone_str: str) -> str:
    """Get timezone abbreviation from timezone string using pytz."""
    if not timezone_str:
        return "UTC"

    try:
        tz = pytz.timezone(timezone_str)
        # Create a sample datetime to get the timezone abbreviation
        sample_time = datetime.now(tz)
        abbr = sample_time.strftime("%Z")
        return abbr if abbr else "UTC"
    except:
        return "UTC"


def convert_utc_to_timezone(utc_time_str: str, timezone_str: str) -> datetime:
    """Convert UTC time string to timezone-aware datetime."""
    if not timezone_str:
        # If no timezone provided, assume UTC
        return datetime.fromisoformat(utc_time_str.replace("Z", "+00:00"))

    try:
        # Parse UTC time
        utc_time = datetime.fromisoformat(utc_time_str.replace("Z", "+00:00"))

        # Create timezone object
        try:
            tz = pytz.timezone(timezone_str)
        except:
            # If timezone is invalid, fall back to UTC
            log.warning(f"Invalid timezone '{timezone_str}', falling back to UTC")
            return utc_time

        # Convert UTC time to the target timezone
        localized_time = utc_time.replace(tzinfo=pytz.UTC)
        converted_time = localized_time.astimezone(tz)

        return converted_time

    except Exception as e:
        log.warning(
            f"Error converting timezone for '{utc_time_str}' to '{timezone_str}': {e}"
        )
        # Fallback to original UTC time
        return datetime.fromisoformat(utc_time_str.replace("Z", "+00:00"))


def format_local_time(
    utc_time_str: str,
    timezone_str: str,
    include_end_time: bool = False,
    end_time_str: Optional[str] = None,
) -> str:
    """Format UTC time as local time with timezone abbreviation."""
    try:
        # Convert start time
        start_time = convert_utc_to_timezone(utc_time_str, timezone_str)

        # Format start time
        start_time_str = start_time.strftime("%I:%M %p")

        if include_end_time and end_time_str:
            # Convert end time
            end_time = convert_utc_to_timezone(end_time_str, timezone_str)
            end_time_str_formatted = end_time.strftime("%I:%M %p")
            time_display = f"{start_time_str} - {end_time_str_formatted}"
        else:
            time_display = start_time_str

        # Get timezone abbreviation
        tz_abbr = get_timezone_abbr(timezone_str)

        return f"{time_display} {tz_abbr}"

    except Exception as e:
        log.warning(f"Error formatting local time: {e}")
        # Fallback to original UTC time
        return f"{utc_time_str} UTC"


# End-User Data Statement for Redbot compliance
__data_statement__ = """
This cog stores the following data for each Discord server (guild) that uses it:

1. Luma API credentials and calendar subscription information
   - Luma API IDs, slugs, and friendly names for subscribed calendars
   - Configuration for channel groups and update intervals
   - Timestamps of when subscriptions were added

2. Data is stored using Red's Config system with the following keys:
   Global settings:
   - update_interval_hours: Integer for update frequency (global for all guilds)
   - last_update: ISO timestamp of last update
   
   Per-guild settings:
   - subscriptions: Dict of API ID -> subscription details
   - channel_groups: Dict of group name -> channel group configuration
   - enabled: Boolean for automatic updates (per guild)

3. No personal user data is collected or stored
4. All data is associated with Discord server IDs
5. Data can be deleted using [p]luma reset command or by removing the cog

This data is necessary for the cog to function as intended - displaying Luma calendar events in Discord channels.
Users have the right to delete this data at any time using the available commands or by removing the cog from their server.
"""


class Luma(commands.Cog):
    """
    Luma Events Plugin for Red-DiscordBot

    A comprehensive cog for managing Luma calendar subscriptions and displaying events across Discord channels.

    Features:
    - Add multiple Luma calendar subscriptions per server
    - Auto-populate calendar slug and name from API
    - Create channel groups to organize event displays
    - Automatic background updates with configurable intervals
    - Manual testing and forced updates
    - Rich embeds with event details and links

    Requires administrator permissions to configure.

    Example usage:
    [p]luma subscriptions add calendar_api_id
    [p]luma groups create "Weekly Events" #general 15
    [p]luma groups addsub "Weekly Events" calendar_api_id
    [p]luma config interval 6
    """

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(
            self, identifier=928374927364, force_registration=True
        )

        # Initialize event database
        self.event_db = EventDatabase(cog_instance=self)

        # Default configuration - global settings
        self.config.register_global(
            update_interval_hours=24,
            last_update=None,
            google_credentials=None,
        )

        # Default guild configuration
        default_guild = {
            "subscriptions": {},
            "channel_groups": {},
            "enabled": True,
            "aggregate_calendar": None,
            "google_event_mapping": {},
        }

        self.config.register_guild(**default_guild)

        # Background tasks
        self.update_task = None
        self.cleanup_task = None
        self.bot.loop.create_task(self.initialize())

    async def initialize(self):
        """Initialize the cog and start background tasks.

        This method is called when the cog is loaded. It waits for the bot
        to be ready and then starts the background update and cleanup tasks.
        """
        await self.bot.wait_until_ready()
        await self.start_update_task()
        self.cleanup_task = self.bot.loop.create_task(self.cleanup_expired_messages())

    def cog_unload(self):
        """Clean up when cog is unloaded."""
        if self.update_task:
            self.update_task.cancel()
        if self.cleanup_task:
            self.cleanup_task.cancel()

    async def start_update_task(self):
        """Start the background task for updating events."""
        if self.update_task and not self.update_task.done():
            self.update_task.cancel()

        self.update_task = self.bot.loop.create_task(self.update_events_loop())

    async def update_events_loop(self):
        """Main loop for updating events from all subscriptions."""
        while True:
            try:
                await self.update_all_events()
                update_interval = await self.config.update_interval_hours()
                await asyncio.sleep(update_interval * 3600)  # Convert hours to seconds
            except Exception as e:
                log.error(f"Error in update events loop: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes before retrying

    async def update_all_events(self):
        """Update events for all guilds and their subscriptions."""
        for guild in self.bot.guilds:
            try:
                # Check if this guild has enabled updates
                if await self.config.guild(guild).enabled():
                    await self.update_guild_events(guild)
                else:
                    log.debug(f"Updates disabled for guild {guild.id}, skipping")
            except Exception as e:
                log.error(f"Error updating events for guild {guild.id}: {e}")

        await self.config.last_update.set(datetime.now(timezone.utc).isoformat())

    async def cleanup_expired_messages(self):
        """Background task to delete Discord messages for expired events.

        Runs every hour and removes messages for events that have already passed.
        """
        await self.bot.wait_until_ready()
        while True:
            try:
                await asyncio.sleep(3600)  # Run every hour

                expired = await self.event_db.get_expired_messages(hours_after_event=2)
                if not expired:
                    continue

                deleted_count = 0
                history_ids_to_remove = []

                for record in expired:
                    try:
                        guild = self.bot.get_guild(record['guild_id'])
                        if not guild:
                            continue

                        channel = guild.get_channel(record['channel_id'])
                        if not channel:
                            continue

                        message_id = record['message_id']
                        if not message_id:
                            continue

                        try:
                            msg = await channel.fetch_message(message_id)
                            await msg.delete()
                            deleted_count += 1
                        except discord.NotFound:
                            pass
                        except discord.Forbidden:
                            log.warning(f"No permission to delete message {message_id}")
                        except discord.HTTPException:
                            pass

                        history_ids_to_remove.append(record['history_id'])

                    except Exception as e:
                        log.debug(f"Error cleaning up message: {e}")

                if history_ids_to_remove:
                    await self.event_db.delete_history_records(history_ids_to_remove)

                if deleted_count > 0:
                    log.info(f"Cleaned up {deleted_count} expired event messages")

            except Exception as e:
                log.error(f"Error in cleanup loop: {e}")
                await asyncio.sleep(300)

    async def update_guild_events(self, guild: discord.Guild):
        """Update events for a specific guild with comprehensive debugging."""
        subscriptions = await self.config.guild(guild).subscriptions()
        channel_groups = await self.config.guild(guild).channel_groups()

        # DEBUG: Log initial state
        log.info(
            f"[DEBUG] update_guild_events for guild {guild.id} ({guild.name}): "
            f"{len(subscriptions)} subscriptions, {len(channel_groups)} channel groups"
        )

        # Check for missing configuration
        if not subscriptions:
            log.warning(
                f"[DEBUG] Guild {guild.id} has no subscriptions configured. "
                "Events cannot be fetched without subscriptions."
            )
            return

        if not channel_groups:
            log.warning(
                f"[DEBUG] Guild {guild.id} has no channel groups configured. "
                "Messages cannot be sent without channel groups."
            )
            return

        messages_sent = 0

        for group_name, group_data in channel_groups.items():
            group = ChannelGroup.from_dict(group_data)

            # DEBUG: Log group details
            log.info(
                f"[DEBUG] Processing group '{group_name}': "
                f"channel_id={group.channel_id}, "
                f"subscriptions={group.subscription_ids}, "
                f"max_events={group.max_events}"
            )

            # Check if group has subscriptions
            if not group.subscription_ids:
                log.warning(
                    f"[DEBUG] Group '{group_name}' has no subscriptions attached. "
                    "Use '[p]luma groups addsub' to add subscriptions."
                )
                continue

            # Verify subscriptions exist in guild config
            valid_subs = [s for s in group.subscription_ids if s in subscriptions]
            if not valid_subs:
                log.warning(
                    f"[DEBUG] Group '{group_name}' references subscriptions that don't exist: "
                    f"{group.subscription_ids}. Available: {list(subscriptions.keys())}"
                )
                continue

            # Check channel exists and permissions
            channel = guild.get_channel(group.channel_id)
            if not channel:
                log.warning(
                    f"[DEBUG] Channel {group.channel_id} for group '{group_name}' not found in guild"
                )
                continue

            if not channel.permissions_for(guild.me).send_messages:
                log.warning(
                    f"[DEBUG] Bot lacks send_messages permission in channel #{channel.name} "
                    f"for group '{group_name}'"
                )
                continue

            result = await self.fetch_events_for_group(
                group, subscriptions, check_for_changes=True
            )

            # DEBUG: Log fetch results
            log.info(
                f"[DEBUG] Group '{group_name}' fetch result: "
                f"events={len(result['events'])}, "
                f"new_events_count={result['new_events_count']}, "
                f"change_stats={result['change_stats']}"
            )

            # Only send message if there are new events
            if result["events"] and result["new_events_count"] > 0:
                log.info(
                    f"Sending {len(result['events'])} new events to group '{group_name}' "
                    f"(detected {result['new_events_count']} new events)"
                )
                await self.send_events_to_channel(
                    group.channel_id, result["events"], guild, group_name,
                    skip_already_sent=True,
                )
                messages_sent += len(result["events"])
            else:
                log.info(
                    f"[DEBUG] No new events for group '{group_name}': "
                    f"events_count={len(result['events'])}, "
                    f"new_events_count={result['new_events_count']}"
                )

        log.info(
            f"[DEBUG] update_guild_events complete for guild {guild.id}: "
            f"Messages Sent: {messages_sent}"
        )

    async def fetch_events_for_group(
        self, group: ChannelGroup, subscriptions: Dict, check_for_changes: bool = True
    ) -> Dict[str, Any]:
        """Fetch events for a specific channel group with change detection."""
        log.debug(
            f"fetch_events_for_group: Processing group '{group.name}', "
            f"subscriptions: {group.subscription_ids}, check_for_changes: {check_for_changes}"
        )

        all_events = []
        seen_api_ids = set()  # Track seen api_ids to prevent duplicates
        all_new_events = []  # Collect new events during initial fetch
        total_new_events = 0
        change_stats = {"new_events": 0, "updated_events": 0, "deleted_events": 0}

        for sub_id in group.subscription_ids:
            if sub_id in subscriptions:
                subscription = Subscription.from_dict(subscriptions[sub_id])
                try:
                    result = await self.fetch_events_from_subscription(
                        subscription, check_for_changes
                    )
                    log.debug(
                        f"Subscription {subscription.name} ({subscription.api_id}): "
                        f"fetched {len(result['events'])} events, "
                        f"{len(result['new_events'])} new events, "
                        f"change_stats: {result['change_stats']}"
                    )

                    # Deduplicate events based on api_id to prevent same event from multiple calendars
                    for event in result["events"]:
                        if event.api_id not in seen_api_ids:
                            all_events.append(event)
                            seen_api_ids.add(event.api_id)
                        else:
                            log.debug(
                                f"Deduplicating event {event.api_id} from subscription {subscription.name}"
                            )

                    # CRITICAL FIX: Collect new events during the initial fetch
                    all_new_events.extend(result["new_events"])
                    total_new_events += len(result["new_events"])

                    # Aggregate change stats
                    for key in change_stats:
                        if key in result["change_stats"]:
                            change_stats[key] += result["change_stats"][key]

                except Exception as e:
                    log.error(f"Error fetching events for subscription {sub_id}: {e}")

        log.debug(
            f"After initial processing: {len(all_events)} unique events, "
            f"{len(all_new_events)} total new events, seen_api_ids: {len(seen_api_ids)}"
        )

        # Sort events by start time and limit to recent events
        all_events.sort(key=lambda x: x.start_at)
        cutoff_date = datetime.now(timezone.utc) - timedelta(
            days=1
        )  # Show events from yesterday onwards

        filtered_events = [
            e
            for e in all_events
            if datetime.fromisoformat(e.start_at.replace("Z", "+00:00")) >= cutoff_date
        ]

        log.debug(f"After filtering by cutoff date: {len(filtered_events)} events")

        # For automatic updates, only include events that are actually new
        if check_for_changes:
            log.debug(
                "Processing for automatic updates - using pre-collected new events"
            )

            # CRITICAL FIX: Use the new events already collected during initial fetch
            # Sort and filter new events
            all_new_events.sort(key=lambda x: x.start_at)
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=1)
            new_filtered_events = [
                e
                for e in all_new_events
                if datetime.fromisoformat(e.start_at.replace("Z", "+00:00"))
                >= cutoff_date
            ]

            # CRITICAL FIX: Deduplicate new events based on api_id to prevent duplicates
            seen_new_api_ids = set()
            deduplicated_new_events = []
            for event in new_filtered_events:
                if event.api_id not in seen_new_api_ids:
                    deduplicated_new_events.append(event)
                    seen_new_api_ids.add(event.api_id)
                else:
                    log.debug(f"Deduplicating NEW event {event.api_id}")

            log.debug(f"New events after deduplication: {len(deduplicated_new_events)}")

            # For automatic updates, return the new events (not all filtered events)
            events_to_return = deduplicated_new_events[: group.max_events]

            # FIX: Use the actual count of new events found (before deduplication)
            # This ensures messages are sent when there are truly new events
            new_events_count = total_new_events

            log.debug(
                f"Final result: {new_events_count} actual new events detected "
                f"(displaying {len(events_to_return)} deduplicated events, "
                f"out of {len(all_new_events)} detected, "
                f"limited to {group.max_events} max per group)"
            )
        else:
            # For manual updates, include all recent events
            events_to_return = filtered_events[: group.max_events]
            new_events_count = total_new_events
            log.debug(
                f"Manual update mode: returning {len(events_to_return)} events, {new_events_count} marked as new"
            )

        return {
            "events": events_to_return,
            "new_events_count": new_events_count,
            "change_stats": change_stats,
        }

    async def fetch_events_from_subscription(
        self, subscription: Subscription, check_for_changes: bool = True
    ) -> Dict[str, Any]:
        """
        Fetch events from a Luma calendar subscription using the real API.

        Args:
            subscription: The subscription to fetch events for
            check_for_changes: Whether to check for changes using the database

        Returns:
            Dict with 'events', 'new_events', 'change_stats'
        """
        try:
            # Create a temporary HTTP session for this request
            async with LumaAPIClient() as client:
                # Fetch events from the calendar using its api_id
                events = await client.get_calendar_events(
                    calendar_identifier=subscription.api_id,
                    limit=100,  # Fetch up to 100 events per subscription
                )

                # Convert events to dict format for database operations
                event_dicts = []
                for event in events:
                    event_dict = {
                        "api_id": event.api_id,
                        "calendar_api_id": subscription.api_id,
                        "name": event.name,
                        "start_at": event.start_at,
                        "end_at": event.end_at,
                        "timezone": event.timezone,
                        # "event_type": event.event_type,
                        "url": event.url,
                        "last_modified": datetime.now(timezone.utc).isoformat(),
                    }
                    event_dicts.append(event_dict)

                if check_for_changes:
                    change_stats = await self.event_db.upsert_events(
                        event_dicts, subscription.api_id
                    )

                    new_event_dicts = change_stats.get("new_event_data", [])
                    new_events = [
                        event
                        for event in events
                        if event.api_id in {e["api_id"] for e in new_event_dicts}
                    ]

                    log.info(
                        f"Successfully fetched {len(events)} events from subscription {subscription.name}. "
                        f"Changes: {change_stats['new_events']} new, {change_stats['updated_events']} updated, "
                        f"{change_stats['deleted_events']} deleted"
                    )

                    return {
                        "events": events,
                        "new_events": new_events,
                        "change_stats": change_stats,
                    }
                else:
                    # Just return all events without change tracking
                    log.info(
                        f"Successfully fetched {len(events)} events from subscription {subscription.name}"
                    )
                    return {
                        "events": events,
                        "new_events": events,  # Treat all as new for manual commands
                        "change_stats": {
                            "new_events": len(events),
                            "updated_events": 0,
                            "deleted_events": 0,
                        },
                    }

        except LumaAPINotFoundError:
            log.error(
                f"Calendar {subscription.slug} not found for subscription {subscription.name}"
            )
            return {"events": [], "new_events": [], "change_stats": {}}

        except LumaAPIRateLimitError:
            log.warning(
                f"Rate limit exceeded while fetching events for subscription {subscription.name}"
            )
            # Return empty list on rate limit - don't crash the entire update process
            return {"events": [], "new_events": [], "change_stats": {}}

        except LumaAPIError as e:
            log.error(
                f"API error while fetching events for subscription {subscription.name}: {e}"
            )
            return {"events": [], "new_events": [], "change_stats": {}}

        except Exception as e:
            log.error(
                f"Unexpected error while fetching events for subscription {subscription.name}: {e}"
            )
            return {"events": [], "new_events": [], "change_stats": {}}

    async def send_events_to_channel(
        self,
        channel_id: int,
        events: List[Event],
        guild: discord.Guild,
        group_name: str,
        skip_already_sent: bool = False,
    ):
        """Send individual event messages to a Discord channel.

        Each new event is sent as its own message/embed for better visibility.

        Args:
            skip_already_sent: If True, skip events already sent to any channel
                              in this guild (prevents cross-group duplicates).
        """
        try:
            channel = guild.get_channel(channel_id)
            if not channel:
                log.warning(f"Channel {channel_id} not found in guild {guild.id}")
                return

            if not events:
                return

            # Filter out events already sent to this guild to prevent cross-group duplicates
            if skip_already_sent:
                sent_event_ids = await self.event_db.get_sent_event_ids_for_guild(guild.id)
                events_to_send = [e for e in events if e.api_id not in sent_event_ids]
                skipped = len(events) - len(events_to_send)
                if skipped > 0:
                    log.info(
                        f"Skipping {skipped} events already sent to guild {guild.id} "
                        f"in group '{group_name}'"
                    )
                if not events_to_send:
                    log.info(
                        f"All {len(events)} events already sent to guild {guild.id}, "
                        f"skipping group '{group_name}'"
                    )
                    return
            else:
                events_to_send = events

            # Get subscriptions for building clickable links
            subscriptions = await self.config.guild(guild).subscriptions()

            # Check if we have permission to send messages
            if not channel.permissions_for(guild.me).send_messages:
                log.warning(f"No permission to send messages in channel {channel_id}")
                return

            # Send each event as an individual message
            for event in events_to_send[:10]:  # Limit to 10 events per update
                try:
                    message = await self._send_single_event_embed(
                        channel, event, subscriptions, group_name, guild.id
                    )
                    # Small delay between messages to avoid rate limiting
                    await asyncio.sleep(0.5)
                except Exception as e:
                    log.error(f"Error sending event {event.name}: {e}")
                    continue

            log.info(
                f"Sent {min(len(events_to_send), 10)} individual event messages to channel {channel_id}"
            )

        except Exception as e:
            log.error(f"Error sending events to channel {channel_id}: {e}")

    async def _send_single_event_embed(
        self,
        channel,
        event: Event,
        subscriptions: Dict,
        group_name: str,
        guild_id: int = None,
    ):
        """Send a single event as its own Discord embed message.

        Returns the sent message object for tracking.
        """
        start_time = datetime.fromisoformat(event.start_at.replace("Z", "+00:00"))

        # Format date/time nicely
        date_str = start_time.strftime("%A, %B %d, %Y")
        time_str = start_time.strftime("%I:%M %p UTC")

        # Build the embed for this single event
        embed = discord.Embed(
            title=f"🆕 New Event: {event.name}",
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc),
        )

        # Find subscription for this event
        subscription = None
        for sub_id, sub_data in subscriptions.items():
            sub = Subscription.from_dict(sub_data)
            if sub.api_id == event.calendar_api_id:
                subscription = sub
                break

        description = ""

        # Use calendar slug from API data (correct data model usage)
        if (
            hasattr(event, "calendar")
            and event.calendar
            and hasattr(event.calendar, "slug")
            and event.calendar.slug
        ):
            # Use the actual Calendar.slug from the API response
            calendar_slug = event.calendar.slug.strip()
            calendar_name = getattr(
                event.calendar,
                "name",
                subscription.name if subscription else "Calendar",
            )
            # Ensure calendar_name is not empty
            if not calendar_name or not calendar_name.strip():
                calendar_name = "Calendar"
            subscription_url = f"https://lu.ma/{calendar_slug}"
            description += f"*from* [{calendar_name}](<{subscription_url}>)\n\n"
        elif subscription and subscription.slug:
            # Fallback to local subscription data if API data not available
            subscription_url = f"https://lu.ma/{subscription.slug}"
            description += f"*from* [{subscription.name}](<{subscription_url}>)\n\n"
        elif subscription:
            # Last resort fallback
            description += f"*from {subscription.name}*\n\n"

        description += f"📅 **Date:** {date_str}\n"

        # Format local time with timezone conversion and abbreviation
        local_time_str = format_local_time(event.start_at, event.timezone or "UTC")
        description += f"🕐 **Local Time:** {local_time_str}\n"

        # Add hosts information if available
        if hasattr(event, "hosts") and event.hosts:
            host_names = [host.name for host in event.hosts[:3]]
            if len(host_names) == 1:
                description += f"👤 **Host:** {host_names[0]}\n"
            elif len(host_names) == 2:
                description += f"👥 **Hosts:** {host_names[0]} & {host_names[1]}\n"
            else:
                description += (
                    f"👥 **Hosts:** {', '.join(host_names[:-1])}, & {host_names[-1]}\n"
                )

        # Add tags information if available
        if hasattr(event, "tags") and event.tags:
            tag_names = [tag.name for tag in event.tags[:3]]  # Limit to 3 tags
            if len(tag_names) == 1:
                description += f"🏷️ **Tag:** {tag_names[0]}\n"
            elif len(tag_names) == 2:
                description += f"🏷️ **Tags:** {tag_names[0]} & {tag_names[1]}\n"
            else:
                description += (
                    f"🏷️ **Tags:** {', '.join(tag_names[:-1])}, & {tag_names[-1]}\n"
                )

        # Build event URL and add link
        event_url = f"https://lu.ma/{event.url}" if event.url else "https://lu.ma"
        description += f"\n🔗 [View Event]({event_url})"

        embed.description = description
        embed.set_footer(text=f"From: {group_name}")

        message = await channel.send(embed=embed)

        # Track the message for cleanup after event expires
        if guild_id and message:
            await self.event_db.record_event_sent(
                event_api_id=event.api_id,
                guild_id=guild_id,
                channel_id=channel.id,
                message_id=message.id,
                start_at=event.start_at,
            )

        return message

    @commands.group(name="luma", invoke_without_command=True)
    @commands.guild_only()
    async def luma_group(self, ctx: commands.Context):
        """Luma Events management commands."""
        if ctx.invoked_subcommand is None:
            embed = discord.Embed(
                title="Luma Events Plugin",
                description="Manage Luma calendar subscriptions and event displays",
                color=discord.Color.blue(),
            )
            embed.add_field(
                name="Commands",
                value="• `subscriptions` - Manage subscriptions\n"
                "• `aggregate` - Aggregate calendar settings\n"
                "• `groups` - Manage channel groups\n"
                "• `events` - Display upcoming events\n"
                "• `schedule` - View update timing and schedule\n"
                "• `config` - Configure update settings",
                inline=False,
            )
            await ctx.send(embed=embed)

    @luma_group.group(name="subscriptions", aliases=["subs"])
    async def subscriptions_group(self, ctx: commands.Context):
        """Manage Luma calendar subscriptions.

        This command group allows you to add, remove, and view your Luma calendar subscriptions.
        Each subscription represents a calendar that events will be fetched from.

        Examples:
        - `[p]luma subscriptions` - View all current subscriptions
        - `[p]luma subscriptions add abc123` - Add a new subscription (auto-populates slug and name)
        - `[p]luma subscriptions remove abc123` - Remove a subscription
        """
        if ctx.invoked_subcommand is None:
            subscriptions = await self.config.guild(ctx.guild).subscriptions()
            if not subscriptions:
                await ctx.send(
                    "No subscriptions configured. Use `[p]luma subscriptions add` to add one."
                )
                return

            embed = discord.Embed(
                title="Current Subscriptions", color=discord.Color.green()
            )

            for sub_id, sub_data in subscriptions.items():
                subscription = Subscription.from_dict(sub_data)
                embed.add_field(
                    name=subscription.name,
                    value=f"API ID: `{subscription.api_id}`\nSlug: `{subscription.slug}`",
                    inline=False,
                )

            await ctx.send(embed=embed)

    @subscriptions_group.command(name="add")
    @checks.admin_or_permissions(manage_guild=True)
    async def add_subscription(self, ctx: commands.Context, *, identifier: str):
        """Add a new Luma calendar subscription.

        Parameters:
        - identifier: Can be any of the following formats:
          - Calendar API ID: `cal-xxxxx`
          - Calendar slug: `genai-ny`
          - ICS URL: `https://api2.luma.com/ics/get?entity=calendar&id=cal-xxxxx`
          - Google Calendar URL: `https://www.google.com/calendar/render?cid=...`
          - Outlook URL: `https://outlook.live.com/calendar/0/addcalendar?url=...`

        The command will automatically extract the calendar ID and fetch
        the calendar's slug and name from the Luma API.

        Example:
        `[p]luma subscriptions add https://api2.luma.com/ics/get?entity=calendar&id=cal-xxxxx`
        """
        # Parse the identifier to extract calendar ID
        api_id = parse_calendar_identifier(identifier)

        subscriptions = await self.config.guild(ctx.guild).subscriptions()

        if api_id in subscriptions:
            await ctx.send(f"A subscription with API ID `{api_id}` already exists.")
            return

        # Send initial message to show progress
        embed = discord.Embed(
            title="Adding Subscription",
            description="🔄 Fetching calendar metadata...",
            color=discord.Color.blue(),
        )
        message = await ctx.send(embed=embed)

        try:
            # Fetch calendar metadata from the API
            async with LumaAPIClient() as client:
                calendar_metadata = await client.get_calendar_metadata_by_api_id(api_id)

                if not calendar_metadata:
                    embed.title = "❌ Failed to Add Subscription"
                    embed.description = (
                        f"Could not fetch calendar metadata for API ID `{api_id}`. "
                        "Please verify the API ID is correct and try again."
                    )
                    embed.color = discord.Color.red()
                    await message.edit(embed=embed)
                    return

                # Extract metadata
                slug = calendar_metadata["slug"]
                name = calendar_metadata["name"]

                # Create subscription with fetched data
                subscription = Subscription(
                    api_id=api_id,
                    slug=slug,
                    name=name,
                    added_by=ctx.author.id,
                    added_at=datetime.now(timezone.utc).isoformat(),
                )

                subscriptions[api_id] = subscription.to_dict()
                await self.config.guild(ctx.guild).subscriptions.set(subscriptions)

                ics_url = f"https://api2.luma.com/ics/get?entity=calendar&id={api_id}"
                google_url = f"https://www.google.com/calendar/render?cid={urllib.parse.quote(ics_url, safe='')}"

                embed.title = "✅ Subscription Added"
                embed.description = f"Successfully added subscription: **{name}**"
                embed.color = discord.Color.green()
                embed.add_field(name="API ID", value=f"`{api_id}`", inline=True)
                embed.add_field(name="Slug", value=f"`{slug}`", inline=True)
                embed.add_field(name="Name", value=f"`{name}`", inline=True)
                embed.add_field(
                    name="📅 Calendar Links",
                    value=f"**ICS:** `{ics_url}`\n[Add to Google Calendar]({google_url})",
                    inline=False,
                )

                # Auto-sync to aggregate Google Calendar if configured
                creds = await self.config.google_credentials()
                aggregate_config = await self.config.guild(ctx.guild).aggregate_calendar()
                
                if creds and aggregate_config:
                    try:
                        from .google_calendar import GoogleCalendarClient
                        client = GoogleCalendarClient(creds)
                        cal_id = aggregate_config.get('calendar_id')

                        async with LumaAPIClient() as api_client:
                            new_events = await api_client.get_calendar_events(
                                calendar_identifier=api_id, limit=50,
                            )

                        now = datetime.now(timezone.utc)
                        upcoming = [
                            e for e in new_events
                            if datetime.fromisoformat(e.start_at.replace("Z", "+00:00")) >= now
                        ]

                        if upcoming:
                            existing_mapping = await self.config.guild(ctx.guild).google_event_mapping()
                            sync_result = await client.sync_events(
                                calendar_id=cal_id,
                                events=upcoming,
                                existing_mapping=existing_mapping,
                            )
                            await self.config.guild(ctx.guild).google_event_mapping.set(sync_result['mapping'])

                            stats = sync_result['stats']
                            embed.add_field(
                                name="Google Calendar",
                                value=f"✅ Synced {stats['created']} new events\n🔁 Updated {stats['updated']} existing",
                                inline=False,
                            )
                            log.info(f"Auto-synced {stats['created']} events from '{name}' to aggregate calendar")
                        else:
                            embed.add_field(
                                name="Google Calendar",
                                value="📋 No upcoming events to sync",
                                inline=False,
                            )
                    except Exception as e:
                        log.warning(f"Error auto-syncing to Google Calendar: {e}")

                await message.edit(embed=embed)

                log.info(
                    f"User {ctx.author.id} added subscription for calendar '{name}' "
                    f"(API ID: {api_id}, Slug: {slug})"
                )

        except LumaAPIRateLimitError:
            embed.title = "⏰ Rate Limited"
            embed.description = (
                "API rate limit exceeded. Please wait a moment and try again."
            )
            embed.color = discord.Color.orange()
            await message.edit(embed=embed)

        except LumaAPINotFoundError:
            embed.title = "❌ Calendar Not Found"
            embed.description = (
                f"Calendar with API ID `{api_id}` was not found. "
                "Please verify the API ID is correct."
            )
            embed.color = discord.Color.red()
            await message.edit(embed=embed)

        except LumaAPIError as e:
            embed.title = "❌ API Error"
            embed.description = f"Failed to fetch calendar metadata: {str(e)}"
            embed.color = discord.Color.red()
            await message.edit(embed=embed)

        except Exception as e:
            log.error(f"Unexpected error in add_subscription: {e}")
            embed.title = "❌ Unexpected Error"
            embed.description = (
                "An unexpected error occurred while adding the subscription."
            )
            embed.color = discord.Color.red()
            await message.edit(embed=embed)

    @subscriptions_group.command(name="remove", aliases=["delete", "del"])
    @checks.admin_or_permissions(manage_guild=True)
    async def remove_subscription(self, ctx: commands.Context, api_id: str):
        """Remove a Luma calendar subscription."""
        subscriptions = await self.config.guild(ctx.guild).subscriptions()

        if api_id not in subscriptions:
            await ctx.send(f"No subscription found with API ID `{api_id}`.")
            return

        subscription = Subscription.from_dict(subscriptions[api_id])
        del subscriptions[api_id]
        await self.config.guild(ctx.guild).subscriptions.set(subscriptions)

        embed = discord.Embed(
            title="Subscription Removed",
            description=f"Successfully removed subscription: **{subscription.name}**",
            color=discord.Color.red(),
        )
        await ctx.send(embed=embed)

    @subscriptions_group.command(name="links", aliases=["calendar", "cal"])
    async def subscription_links(self, ctx: commands.Context):
        """Get calendar subscribe links for all subscriptions.

        Shows ICS feed URLs and Google Calendar subscribe links for each
        configured subscription. Use these to add calendars to your preferred
        calendar application.

        Example:
        `[p]luma subscriptions links`
        """
        subscriptions = await self.config.guild(ctx.guild).subscriptions()

        if not subscriptions:
            await ctx.send(
                "No subscriptions configured. Use `[p]luma subscriptions add` to add one."
            )
            return

        embed = discord.Embed(
            title="📅 Calendar Subscribe Links",
            description="Use these links to add calendars to your calendar app:",
            color=discord.Color.blue(),
        )
        for sub_id, sub_data in subscriptions.items():
            subscription = Subscription.from_dict(sub_data)
            api_id = subscription.api_id

            ics_url = f"https://api2.luma.com/ics/get?entity=calendar&id={api_id}"
            google_url = f"https://www.google.com/calendar/render?cid={urllib.parse.quote(ics_url, safe='')}"

            embed.add_field(
                name=subscription.name,
                value=f"**ICS Feed:**\n`{ics_url}`\n\n[Add to Google Calendar]({google_url})",
                inline=False,
            )

        embed.set_footer(text="ICS feeds update automatically when events change")
        await ctx.send(embed=embed)

    @luma_group.group(name="aggregate", aliases=["agg"])
    async def aggregate_group(self, ctx: commands.Context):
        """Manage the aggregate Google Calendar.

        The aggregate calendar combines all your Luma subscriptions into one
        Google Calendar for easy viewing.

        Setup:
        1. Create a Google Calendar (or use existing)
        2. Get the Calendar ID from settings (looks like: xxx@group.calendar.google.com)
        3. Run `[p]luma aggregate setup <calendar_id>`
        4. Share the calendar with the bot's service account (if using service account auth)

        Example:
        - `[p]luma aggregate setup abc123@group.calendar.google.com`
        - `[p]luma aggregate link` - Get the view link
        """
        if ctx.invoked_subcommand is None:
            aggregate_config = await self.config.guild(ctx.guild).aggregate_calendar()

            embed = discord.Embed(
                title="📅 Aggregate Calendar",
                color=discord.Color.blue(),
            )

            if aggregate_config:
                cal_id = aggregate_config.get("calendar_id")
                embed.description = f"Aggregate calendar is configured."
                embed.add_field(
                    name="Calendar ID",
                    value=f"`{cal_id}`",
                    inline=False,
                )
                embed.add_field(
                    name="View Calendar",
                    value=f"[Open in Google Calendar](https://calendar.google.com/calendar?cid={cal_id.replace('@', '%40')})",
                    inline=False,
                )
                embed.add_field(
                    name="Manage",
                    value="• `[p]luma aggregate link` - Get shareable link\n"
                    "• `[p]luma aggregate sync` - Sync all subscriptions\n"
                    "• `[p]luma aggregate clear` - Clear configuration",
                    inline=False,
                )
            else:
                embed.description = "No aggregate calendar configured."
                embed.add_field(
                    name="Setup",
                    value="Use `[p]luma aggregate setup <calendar_id>` to configure.\n"
                    "Example: `[p]luma aggregate setup abc123@group.calendar.google.com`",
                    inline=False,
                )

            await ctx.send(embed=embed)

    @aggregate_group.command(name="setup")
    @checks.admin_or_permissions(manage_guild=True)
    async def aggregate_setup(self, ctx: commands.Context, calendar_id: str):
        """Set up the aggregate Google Calendar.

        Parameters:
        - calendar_id: The Google Calendar ID (e.g., abc123@group.calendar.google.com)

        Find your Google Calendar ID:
        1. Go to Google Calendar settings
        2. Click on your calendar
        3. Find "Integrate calendar" section
        4. Copy the "Calendar ID"

        Example:
        `[p]luma aggregate setup abc123@group.calendar.google.com`
        """
        existing = await self.config.guild(ctx.guild).aggregate_calendar()
        aggregate_config = {
            "calendar_id": calendar_id,
            "setup_by": ctx.author.id,
            "setup_at": datetime.now(timezone.utc).isoformat(),
            "synced_subscriptions": existing.get("synced_subscriptions", []) if existing else [],
        }

        await self.config.guild(ctx.guild).aggregate_calendar.set(aggregate_config)

        embed = discord.Embed(
            title="✅ Aggregate Calendar Configured",
            color=discord.Color.green(),
        )
        embed.add_field(
            name="Calendar ID",
            value=f"`{calendar_id}`",
            inline=False,
        )
        embed.add_field(
            name="View Calendar",
            value=f"[Open in Google Calendar](https://calendar.google.com/calendar?cid={calendar_id.replace('@', '%40')})",
            inline=False,
        )
        embed.add_field(
            name="Next Steps",
            value="1. Add subscriptions with `[p]luma subscriptions add <api_id>`\n"
            "2. Each subscription's ICS feed will be added to this calendar\n"
            "3. Use `[p]luma aggregate link` to get the shareable link",
            inline=False,
        )

        await ctx.send(embed=embed)

    @aggregate_group.command(name="link", aliases=["url", "view"])
    async def aggregate_link(self, ctx: commands.Context):
        """Get the link to view the aggregate calendar.

        Shows both the Google Calendar view link and embed code if you want
        to display the calendar on a website.
        """
        aggregate_config = await self.config.guild(ctx.guild).aggregate_calendar()

        if not aggregate_config:
            await ctx.send(
                "No aggregate calendar configured. Use `[p]luma aggregate setup <calendar_id>` first."
            )
            return

        cal_id = aggregate_config.get("calendar_id")
        subscriptions = await self.config.guild(ctx.guild).subscriptions()

        embed = discord.Embed(
            title="📅 Aggregate Calendar Link",
            color=discord.Color.blue(),
        )

        embed.add_field(
            name="View in Google Calendar",
            value=f"[Open Calendar](https://calendar.google.com/calendar?cid={cal_id.replace('@', '%40')})",
            inline=False,
        )

        embed.add_field(
            name="Public Link (if calendar is public)",
            value=f"https://calendar.google.com/calendar/embed?src={cal_id}",
            inline=False,
        )

        embed.add_field(
            name="Synced Subscriptions",
            value=f"{len(subscriptions)} calendars added",
            inline=True,
        )

        embed.set_footer(text="Make sure your Google Calendar is shared publicly for the public link to work")

        await ctx.send(embed=embed)

    @aggregate_group.command(name="credentials", aliases=["creds"])
    @checks.is_owner()
    async def aggregate_credentials(self, ctx: commands.Context, credentials_path: str = None):
        """Configure Google Calendar API credentials.

        This is a bot owner only command. Credentials are stored globally.
        Running with a new path will replace existing credentials.

        Setup:
        1. Go to console.cloud.google.com
        2. Create a Service Account with Google Calendar API enabled
        3. Download the JSON credentials file
        4. Run this command with the file path

        Parameters:
        - credentials_path: Path to the credentials JSON file, or "clear" to remove

        Example:
        `[p]luma aggregate credentials /path/to/credentials.json`
        `[p]luma aggregate credentials clear`
        """
        import os
        import json

        if credentials_path and credentials_path.lower() == "clear":
            await self.config.google_credentials.clear()
            await ctx.send("✅ Google credentials removed.")
            log.info(f"Google credentials cleared by bot owner {ctx.author.id}")
            return

        if credentials_path:
            if not os.path.exists(credentials_path):
                await ctx.send(f"❌ File not found: `{credentials_path}`")
                return

            try:
                with open(credentials_path, 'r') as f:
                    creds_data = json.load(f)

                required_fields = ['type', 'project_id', 'private_key', 'client_email']
                missing = [f for f in required_fields if f not in creds_data]
                if missing:
                    await ctx.send(f"❌ Invalid credentials file. Missing fields: {', '.join(missing)}")
                    return

                await self.config.google_credentials.set(creds_data)

                embed = discord.Embed(
                    title="✅ Google Credentials Configured",
                    description="Run `[p]luma aggregate test` to verify the connection.",
                    color=discord.Color.green(),
                )
                embed.add_field(
                    name="Service Account",
                    value=f"`{creds_data['client_email']}`",
                    inline=False,
                )
                embed.add_field(
                    name="Project ID",
                    value=f"`{creds_data['project_id']}`",
                    inline=True,
                )
                embed.add_field(
                    name="Next Step",
                    value=f"1. Share your Google Calendar with:\n`{creds_data['client_email']}`\n\n2. Give it 'Make changes to events' permission\n\n3. Run `[p]luma aggregate test` to verify",
                    inline=False,
                )

                await ctx.send(embed=embed)
                log.info(f"Google credentials configured by bot owner {ctx.author.id}")

            except json.JSONDecodeError:
                await ctx.send("❌ Invalid JSON file. Please check the file format.")
            except Exception as e:
                log.error(f"Error loading credentials: {e}")
                await ctx.send(f"❌ Error loading credentials: {str(e)}")

        else:
            creds = await self.config.google_credentials()
            if creds:
                embed = discord.Embed(
                    title="Google Credentials Status",
                    color=discord.Color.blue(),
                )
                embed.add_field(
                    name="Service Account",
                    value=f"`{creds.get('client_email', 'Unknown')}`",
                    inline=False,
                )
                embed.add_field(
                    name="Project ID",
                    value=f"`{creds.get('project_id', 'Unknown')}`",
                    inline=True,
                )
                embed.add_field(
                    name="Status",
                    value="✅ Configured",
                    inline=True,
                )
                embed.add_field(
                    name="Manage",
                    value=f"• Replace: `{ctx.prefix}luma aggregate credentials <new_path>`\n"
                          f"• Remove: `{ctx.prefix}luma aggregate credentials clear`\n"
                          f"• Test: `{ctx.prefix}luma aggregate test`",
                    inline=False,
                )
                await ctx.send(embed=embed)
            else:
                env_creds = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
                if env_creds:
                    await ctx.send(
                        f"📋 Using credentials from environment variable:\n`GOOGLE_APPLICATION_CREDENTIALS={env_creds}`\n\n"
                        f"Or use `{ctx.prefix}luma aggregate credentials <path>` to configure directly."
                    )
                else:
                    await ctx.send(
                        "❌ No Google credentials configured.\n\n"
                        f"Usage: `{ctx.prefix}luma aggregate credentials /path/to/credentials.json`\n\n"
                        "Or set environment variable:\n"
                        "`GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json`"
                    )

    @aggregate_group.command(name="sync")
    @checks.admin_or_permissions(manage_guild=True)
    async def aggregate_sync(self, ctx: commands.Context):
        """Sync all subscriptions to the aggregate Google Calendar.

        Fetches events from all Luma subscriptions and creates/updates
        them in the aggregate Google Calendar.

        Requirements:
        - Google credentials configured (`[p]luma aggregate credentials`)
        - Aggregate calendar set up (`[p]luma aggregate setup`)
        - Service account has access to your Google Calendar
        """
        from .google_calendar import GoogleCalendarClient

        creds = await self.config.google_credentials()
        if not creds:
            env_creds = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
            if not env_creds:
                await ctx.send(
                    "❌ Google credentials not configured.\n"
                    f"Use `{ctx.prefix}luma aggregate credentials <path>` first."
                )
                return

        aggregate_config = await self.config.guild(ctx.guild).aggregate_calendar()
        if not aggregate_config:
            await ctx.send(
                "❌ No aggregate calendar configured.\n"
                f"Use `{ctx.prefix}luma aggregate setup <calendar_id>` first."
            )
            return

        subscriptions = await self.config.guild(ctx.guild).subscriptions()
        if not subscriptions:
            await ctx.send("❌ No subscriptions to sync. Add some with `[p]luma subscriptions add`")
            return

        status_msg = await ctx.send("🔄 Syncing events to Google Calendar...")

        try:
            client = GoogleCalendarClient(creds)

            test_result = await client.test_connection()
            if not test_result.get('success'):
                await status_msg.edit(content=f"❌ Google Calendar connection failed: {test_result.get('error')}")
                return

            cal_id = aggregate_config.get('calendar_id')
            existing_mapping = await self.config.guild(ctx.guild).google_event_mapping()

            # Fetch events from all subscriptions
            all_events = []
            seen_api_ids = set()
            async with LumaAPIClient() as api_client:
                for sub_id, sub_data in subscriptions.items():
                    subscription = Subscription.from_dict(sub_data)
                    try:
                        events = await api_client.get_calendar_events(
                            calendar_identifier=subscription.api_id,
                            limit=100,
                        )
                        for event in events:
                            if event.api_id not in seen_api_ids:
                                all_events.append(event)
                                seen_api_ids.add(event.api_id)
                    except Exception as e:
                        log.warning(f"Error fetching events for {subscription.name}: {e}")

            # Filter to future events only
            now = datetime.now(timezone.utc)
            upcoming = [
                e for e in all_events
                if datetime.fromisoformat(e.start_at.replace("Z", "+00:00")) >= now
            ]
            upcoming.sort(key=lambda x: x.start_at)

            if not upcoming:
                await status_msg.edit(content="📋 No upcoming events to sync.")
                return

            # Sync events to Google Calendar
            sync_result = await client.sync_events(
                calendar_id=cal_id,
                events=upcoming,
                existing_mapping=existing_mapping,
            )

            # Save updated mapping
            await self.config.guild(ctx.guild).google_event_mapping.set(sync_result['mapping'])

            stats = sync_result['stats']
            embed = discord.Embed(
                title="📅 Sync Complete",
                color=discord.Color.green() if stats['failed'] == 0 else discord.Color.orange(),
            )
            embed.add_field(
                name="Results",
                value=f"✅ {stats['created']} created\n🔁 {stats['updated']} updated\n⏭️ {stats['skipped']} skipped\n❌ {stats['failed']} failed",
                inline=False,
            )
            embed.add_field(
                name="Total Events",
                value=f"{len(upcoming)} upcoming events across {len(subscriptions)} calendars",
                inline=True,
            )
            embed.add_field(
                name="View Calendar",
                value=f"[Open in Google Calendar](https://calendar.google.com/calendar?cid={cal_id.replace('@', '%40')})",
                inline=False,
            )

            if stats['errors']:
                error_list = "\n".join(f"• {e}" for e in stats['errors'][:5])
                if len(error_list) > 1024:
                    error_list = error_list[:1020] + "\n..."
                embed.add_field(name="Errors", value=error_list, inline=False)

            await status_msg.edit(content=None, embed=embed)

        except ImportError as e:
            await status_msg.edit(content=f"❌ Missing dependencies: {str(e)}\nRun: `pip install google-api-python-client google-auth`")
        except Exception as e:
            log.error(f"Error syncing to Google Calendar: {e}")
            await status_msg.edit(content=f"❌ Error: {str(e)}")

    @aggregate_group.command(name="test")
    @checks.admin_or_permissions(manage_guild=True)
    async def aggregate_test(self, ctx: commands.Context):
        """Test Google Calendar API connection."""
        import os
        from .google_calendar import GoogleCalendarClient

        creds = await self.config.google_credentials()

        if not creds:
            env_creds = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
            if not env_creds:
                await ctx.send(
                    "❌ No credentials configured.\n"
                    f"Use `{ctx.prefix}luma aggregate credentials <path>`"
                )
                return

        try:
            client = GoogleCalendarClient(creds)
            result = await client.test_connection()

            if result.get('success'):
                embed = discord.Embed(
                    title="✅ Google Calendar Connection OK",
                    color=discord.Color.green(),
                )
                embed.add_field(
                    name="Service Account",
                    value=f"`{result.get('service_account', 'Unknown')}`",
                    inline=False,
                )

                calendars = await client.list_calendars()
                embed.add_field(
                    name="Calendars Accessible",
                    value=f"{len(calendars)} calendars",
                    inline=True,
                )

                await ctx.send(embed=embed)
            else:
                await ctx.send(f"❌ Connection failed: {result.get('error')}")

        except ImportError as e:
            await ctx.send(f"❌ Missing dependencies: {str(e)}\nRun: `pip install google-api-python-client google-auth`")
        except Exception as e:
            await ctx.send(f"❌ Error: {str(e)}")

    @aggregate_group.command(name="migrate")
    @checks.admin_or_permissions(manage_guild=True)
    async def aggregate_migrate(self, ctx: commands.Context):
        """Migrate existing subscriptions to aggregate calendar.

        This command will:
        1. Sync all events to Google Calendar
        2. Clear the event database (to reset "new event" tracking)
        3. Trigger a force update to blast all events to channels

        Use this when setting up the aggregate calendar for the first time
        with existing subscriptions.
        """
        from .google_calendar import GoogleCalendarClient

        creds = await self.config.google_credentials()
        aggregate_config = await self.config.guild(ctx.guild).aggregate_calendar()
        subscriptions = await self.config.guild(ctx.guild).subscriptions()

        if not subscriptions:
            await ctx.send("❌ No subscriptions to migrate.")
            return

        embed = discord.Embed(
            title="🔄 Migration Started",
            description="Migrating existing subscriptions to aggregate calendar...",
            color=discord.Color.blue(),
        )
        status_msg = await ctx.send(embed=embed)

        results = {
            'google_sync': None,
            'database_cleared': False,
            'events_blasted': 0,
            'errors': [],
        }

        # Step 1: Sync events to Google Calendar if configured
        if creds and aggregate_config:
            try:
                client = GoogleCalendarClient(creds)
                cal_id = aggregate_config.get('calendar_id')
                existing_mapping = await self.config.guild(ctx.guild).google_event_mapping()

                all_events = []
                seen_api_ids = set()
                async with LumaAPIClient() as api_client:
                    for sub_id, sub_data in subscriptions.items():
                        subscription = Subscription.from_dict(sub_data)
                        try:
                            events = await api_client.get_calendar_events(
                                calendar_identifier=subscription.api_id, limit=100,
                            )
                            for event in events:
                                if event.api_id not in seen_api_ids:
                                    all_events.append(event)
                                    seen_api_ids.add(event.api_id)
                        except Exception as e:
                            results['errors'].append(f"Fetch {subscription.name}: {str(e)}")

                now = datetime.now(timezone.utc)
                upcoming = [
                    e for e in all_events
                    if datetime.fromisoformat(e.start_at.replace("Z", "+00:00")) >= now
                ]
                upcoming.sort(key=lambda x: x.start_at)

                if upcoming:
                    sync_result = await client.sync_events(
                        calendar_id=cal_id,
                        events=upcoming,
                        existing_mapping=existing_mapping,
                    )
                    await self.config.guild(ctx.guild).google_event_mapping.set(sync_result['mapping'])
                    stats = sync_result['stats']
                    results['google_sync'] = {
                        'created': stats['created'],
                        'updated': stats['updated'],
                        'failed': stats['failed'],
                        'total': len(upcoming),
                    }
                else:
                    results['google_sync'] = {'created': 0, 'updated': 0, 'failed': 0, 'total': 0}

            except Exception as e:
                results['errors'].append(f"Google sync: {str(e)}")

        # Step 2: Clear event database
        try:
            calendar_ids = list(subscriptions.keys())
            clear_result = await self.event_db.clear_event_database(calendar_ids)
            results['database_cleared'] = clear_result.get('success', False)
        except Exception as e:
            results['errors'].append(f"Database clear: {str(e)}")

        # Step 3: Force update to blast events
        try:
            channel_groups = await self.config.guild(ctx.guild).channel_groups()

            for group_name, group_data in channel_groups.items():
                group = ChannelGroup.from_dict(group_data)
                result = await self.fetch_events_for_group(group, subscriptions, check_for_changes=False)

                if result['events']:
                    await self.send_events_to_channel(
                        group.channel_id, result['events'], ctx.guild, group_name
                    )
                    results['events_blasted'] += len(result['events'])

        except Exception as e:
            results['errors'].append(f"Event blast: {str(e)}")

        # Build result embed
        embed.title = "✅ Migration Complete"
        embed.color = discord.Color.green() if not results['errors'] else discord.Color.orange()

        if results['google_sync']:
            gs = results['google_sync']
            embed.add_field(
                name="Google Calendar",
                value=f"✅ {gs['created']} created\n🔁 {gs['updated']} updated\n❌ {gs['failed']} failed\n📊 {gs['total']} total",
                inline=True,
            )
        else:
            embed.add_field(
                name="Google Calendar",
                value="⏭️ Skipped (not configured)",
                inline=True,
            )

        embed.add_field(
            name="Database",
            value="✅ Cleared" if results['database_cleared'] else "❌ Failed",
            inline=True,
        )

        embed.add_field(
            name="Events Blasted",
            value=f"📢 {results['events_blasted']} events sent to channels",
            inline=True,
        )

        if results['errors']:
            embed.add_field(
                name="Errors",
                value="\n".join(f"• {e}" for e in results['errors'][:5]),
                inline=False,
            )

        await status_msg.edit(embed=embed)

    @aggregate_group.command(name="clear", aliases=["reset", "delete"])
    @checks.admin_or_permissions(manage_guild=True)
    async def aggregate_clear(self, ctx: commands.Context):
        """Clear the aggregate calendar configuration.

        This removes the configuration and event mapping from the bot.
        Events already created in Google Calendar remain there.
        Use `[p]luma aggregate purge` to also delete events from Google Calendar.
        """
        await self.config.guild(ctx.guild).aggregate_calendar.set(None)
        await self.config.guild(ctx.guild).google_event_mapping.set({})

        embed = discord.Embed(
            title="Aggregate Calendar Cleared",
            description="Configuration and event mapping removed.\nNote: Events in Google Calendar remain there.",
            color=discord.Color.orange(),
        )
        await ctx.send(embed=embed)

    @aggregate_group.command(name="purge")
    @checks.admin_or_permissions(manage_guild=True)
    async def aggregate_purge(self, ctx: commands.Context):
        """Delete all synced events from the aggregate Google Calendar.

        This removes all events that were created by the bot from the
        target Google Calendar and clears the event mapping.
        """
        from .google_calendar import GoogleCalendarClient

        creds = await self.config.google_credentials()
        aggregate_config = await self.config.guild(ctx.guild).aggregate_calendar()

        if not creds or not aggregate_config:
            await ctx.send("❌ No aggregate calendar configured.")
            return

        mapping = await self.config.guild(ctx.guild).google_event_mapping()
        if not mapping:
            await ctx.send("📋 No synced events to purge.")
            return

        cal_id = aggregate_config.get('calendar_id')
        status_msg = await ctx.send(f"🔄 Purging {len(mapping)} events from Google Calendar...")

        try:
            client = GoogleCalendarClient(creds)
            result = await client.clear_calendar(cal_id, mapping)

            await self.config.guild(ctx.guild).google_event_mapping.set({})

            embed = discord.Embed(
                title="🗑️ Events Purged",
                color=discord.Color.orange(),
            )
            embed.add_field(
                name="Results",
                value=f"✅ {result['deleted']} deleted\n❌ {result['failed']} failed",
                inline=False,
            )
            await status_msg.edit(content=None, embed=embed)

        except Exception as e:
            log.error(f"Error purging Google Calendar events: {e}")
            await status_msg.edit(content=f"❌ Error: {str(e)}")

    @luma_group.group(name="groups")
    async def groups_group(self, ctx: commands.Context):
        """Manage channel groups for displaying events.

        This command group allows you to create, configure, and delete channel groups
        for organizing event displays across your Discord server.

        Examples:
        - `[p]luma groups` - View all current channel groups
        - `[p]luma groups create "Weekly Events" #general 15` - Create a new group
        - `[p]luma groups delete "Weekly Events"` - Delete a group
        - `[p]luma groups addsub "Weekly Events" calendar_api_id` - Add subscription to group
        """
        if ctx.invoked_subcommand is None:
            channel_groups = await self.config.guild(ctx.guild).channel_groups()
            if not channel_groups:
                await ctx.send(
                    "No channel groups configured. Use `create` to create one."
                )
                return

            embed = discord.Embed(
                title="Current Channel Groups", color=discord.Color.blue()
            )

            for group_name, group_data in channel_groups.items():
                group = ChannelGroup.from_dict(group_data)
                channel = ctx.guild.get_channel(group.channel_id)
                channel_name = (
                    channel.name if channel else f"Unknown Channel ({group.channel_id})"
                )

                embed.add_field(
                    name=group_name,
                    value=f"Channel: #{channel_name}\nSubscriptions: {len(group.subscription_ids)}\nMax Events: {group.max_events}",
                    inline=False,
                )

            await ctx.send(embed=embed)

    @groups_group.command(name="create")
    @checks.admin_or_permissions(manage_guild=True)
    async def create_group(
        self,
        ctx: commands.Context,
        name: str,
        channel: discord.TextChannel,
        max_events: int = 10,
        group_timezone: Optional[str] = None,
    ):
        """Create a new channel group for displaying events.

        Parameters:
        - name: Name of the group
        - channel: Discord channel to display events in
        - max_events: Maximum number of events to show (default: 10)
        - group_timezone: Optional timezone for displaying event times (e.g., 'America/New_York')
        """
        channel_groups = await self.config.guild(ctx.guild).channel_groups()

        if name in channel_groups:
            await ctx.send(f"A channel group named `{name}` already exists.")
            return

        group = ChannelGroup(
            name=name,
            channel_id=channel.id,
            subscription_ids=[],
            max_events=max_events,
            created_by=ctx.author.id,
            created_at=datetime.now(timezone.utc).isoformat(),
            timezone=group_timezone,
        )

        channel_groups[name] = group.to_dict()
        await self.config.guild(ctx.guild).channel_groups.set(channel_groups)

        embed = discord.Embed(
            title="Channel Group Created",
            description=f"Successfully created group: **{name}**",
            color=discord.Color.green(),
        )
        embed.add_field(name="Channel", value=channel.mention, inline=True)
        embed.add_field(name="Max Events", value=str(max_events), inline=True)
        if group_timezone:
            embed.add_field(name="Timezone", value=group_timezone, inline=True)
        else:
            embed.add_field(name="Timezone", value="Default (from event)", inline=True)

        await ctx.send(embed=embed)

    @groups_group.command(name="timezone", aliases=["tz"])
    @checks.admin_or_permissions(manage_guild=True)
    async def set_group_timezone(
        self, ctx: commands.Context, group_name: str, timezone: str
    ):
        """Set timezone for a channel group.

        Parameters:
        - group_name: Name of the group to update
        - timezone: Timezone to use (e.g., 'America/New_York', 'UTC')
        """
        channel_groups = await self.config.guild(ctx.guild).channel_groups()

        if group_name not in channel_groups:
            await ctx.send(f"No channel group found named `{group_name}`.")
            return

        group = ChannelGroup.from_dict(channel_groups[group_name])
        group.timezone = timezone

        channel_groups[group_name] = group.to_dict()
        await self.config.guild(ctx.guild).channel_groups.set(channel_groups)

        embed = discord.Embed(
            title="Group Timezone Updated",
            description=f"Updated timezone for group: **{group_name}**",
            color=discord.Color.green(),
        )
        embed.add_field(name="Timezone", value=timezone, inline=True)

        await ctx.send(embed=embed)

    @groups_group.group(name="edit", invoke_without_command=False)
    @checks.admin_or_permissions(manage_guild=True)
    async def edit_group(self, ctx: commands.Context):
        """Edit properties of a channel group.

        Subcommands:
        - name: Change the group name
        - channel: Change the target channel
        - max: Change maximum number of events
        - timezone: Change the group timezone

        Examples:
        - `[p]luma groups edit "Weekly Events" name "New Name"`
        - `[p]luma groups edit "Weekly Events" channel #new-channel`
        - `[p]luma groups edit "Weekly Events" max 15`
        - `[p]luma groups edit "Weekly Events" timezone "America/New_York"`
        """
        if ctx.invoked_subcommand is None:
            embed = discord.Embed(
                title="Edit Group Subcommands",
                description="Available subcommands for editing groups:",
                color=discord.Color.blue(),
            )
            embed.add_field(
                name="name",
                value='Change the group name\n`[p]luma groups edit "Group Name" name "New Name"`',
                inline=False,
            )
            embed.add_field(
                name="channel",
                value='Change the target channel\n`[p]luma groups edit "Group Name" channel #new-channel`',
                inline=False,
            )
            embed.add_field(
                name="max",
                value='Change maximum number of events\n`[p]luma groups edit "Group Name" max 15`',
                inline=False,
            )
            embed.add_field(
                name="timezone",
                value='Change the group timezone\n`[p]luma groups edit "Group Name" timezone "America/New_York"`',
                inline=False,
            )
            await ctx.send(embed=embed)

    @edit_group.command(name="name")
    @checks.admin_or_permissions(manage_guild=True)
    async def edit_group_name(
        self, ctx: commands.Context, group_name: str, new_name: str
    ):
        """Change the name of a channel group.

        Parameters:
        - group_name: Current name of the group to edit
        - new_name: New name for the group

        Example:
        `[p]luma groups edit "Weekly Events" name "New Events Channel"`
        """
        channel_groups = await self.config.guild(ctx.guild).channel_groups()

        if group_name not in channel_groups:
            await ctx.send(f"No channel group found named `{group_name}`.")
            return

        if new_name in channel_groups:
            await ctx.send(f"A channel group named `{new_name}` already exists.")
            return

        # Get the group data and update the name
        group_data = channel_groups[group_name]
        group = ChannelGroup.from_dict(group_data)

        # Update the group name
        group.name = new_name
        channel_groups[new_name] = group.to_dict()
        del channel_groups[group_name]

        await self.config.guild(ctx.guild).channel_groups.set(channel_groups)

        embed = discord.Embed(
            title="Group Name Updated",
            description=f"Renamed group from **{group_name}** to **{new_name}**",
            color=discord.Color.green(),
        )
        await ctx.send(embed=embed)

    @edit_group.command(name="channel")
    @checks.admin_or_permissions(manage_guild=True)
    async def edit_group_channel(
        self, ctx: commands.Context, group_name: str, channel: discord.TextChannel
    ):
        """Change the target channel for a channel group.

        Parameters:
        - group_name: Name of the group to edit
        - channel: New Discord channel to display events in

        Example:
        `[p]luma groups edit "Weekly Events" channel #events-channel`
        """
        channel_groups = await self.config.guild(ctx.guild).channel_groups()

        if group_name not in channel_groups:
            await ctx.send(f"No channel group found named `{group_name}`.")
            return

        group = ChannelGroup.from_dict(channel_groups[group_name])
        old_channel = ctx.guild.get_channel(group.channel_id)
        old_channel_name = (
            f"#{old_channel.name}"
            if old_channel
            else f"Unknown Channel ({group.channel_id})"
        )

        # Update the channel
        group.channel_id = channel.id
        channel_groups[group_name] = group.to_dict()
        await self.config.guild(ctx.guild).channel_groups.set(channel_groups)

        embed = discord.Embed(
            title="Group Channel Updated",
            description=f"Updated channel for group **{group_name}**",
            color=discord.Color.green(),
        )
        embed.add_field(name="Old Channel", value=old_channel_name, inline=True)
        embed.add_field(name="New Channel", value=channel.mention, inline=True)
        await ctx.send(embed=embed)

    @edit_group.command(name="max")
    @checks.admin_or_permissions(manage_guild=True)
    async def edit_group_max(
        self, ctx: commands.Context, group_name: str, max_events: int
    ):
        """Change the maximum number of events for a channel group.

        Parameters:
        - group_name: Name of the group to edit
        - max_events: New maximum number of events to display (1-50)

        Example:
        `[p]luma groups edit "Weekly Events" max 15`
        """
        if not 1 <= max_events <= 50:
            await ctx.send("Maximum events must be between 1 and 50.")
            return

        channel_groups = await self.config.guild(ctx.guild).channel_groups()

        if group_name not in channel_groups:
            await ctx.send(f"No channel group found named `{group_name}`.")
            return

        group = ChannelGroup.from_dict(channel_groups[group_name])
        old_max = group.max_events

        # Update the max events
        group.max_events = max_events
        channel_groups[group_name] = group.to_dict()
        await self.config.guild(ctx.guild).channel_groups.set(channel_groups)

        embed = discord.Embed(
            title="Group Max Events Updated",
            description=f"Updated max events for group **{group_name}**",
            color=discord.Color.green(),
        )
        embed.add_field(name="Old Max", value=str(old_max), inline=True)
        embed.add_field(name="New Max", value=str(max_events), inline=True)
        await ctx.send(embed=embed)

    @edit_group.command(name="timezone", aliases=["tz"])
    @checks.admin_or_permissions(manage_guild=True)
    async def edit_group_timezone(
        self, ctx: commands.Context, group_name: str, timezone: str
    ):
        """Change the timezone for a channel group.

        Parameters:
        - group_name: Name of the group to edit
        - timezone: New timezone (e.g., 'America/New_York', 'UTC')

        Example:
        `[p]luma groups edit "Weekly Events" timezone "America/New_York"`
        """
        channel_groups = await self.config.guild(ctx.guild).channel_groups()

        if group_name not in channel_groups:
            await ctx.send(f"No channel group found named `{group_name}`.")
            return

        # Validate timezone
        try:
            pytz.timezone(timezone)
        except:
            await ctx.send(
                f"Invalid timezone '{timezone}'. Please use a valid timezone like 'America/New_York' or 'UTC'."
            )
            return

        group = ChannelGroup.from_dict(channel_groups[group_name])
        old_timezone = group.timezone or "Default (from event)"

        # Update the timezone
        group.timezone = timezone
        channel_groups[group_name] = group.to_dict()
        await self.config.guild(ctx.guild).channel_groups.set(channel_groups)

        embed = discord.Embed(
            title="Group Timezone Updated",
            description=f"Updated timezone for group **{group_name}**",
            color=discord.Color.green(),
        )
        embed.add_field(name="Old Timezone", value=old_timezone, inline=True)
        embed.add_field(name="New Timezone", value=timezone, inline=True)
        await ctx.send(embed=embed)

    @groups_group.command(name="addsub", aliases=["addsubscription"])
    @checks.admin_or_permissions(manage_guild=True)
    async def add_subscription_to_group(
        self, ctx: commands.Context, group_name: str, subscription_identifier: str
    ):
        """Add a subscription to a channel group using either API ID or calendar slug.

        Parameters:
        - group_name: Name of the group to add the subscription to
        - subscription_identifier: Either the API ID (e.g., 'cal-r8BcsXhhHYmA3tp')
          or calendar slug (e.g., 'genai-ny') of the subscription

        This command now accepts both API IDs and calendar slugs for flexibility.
        If you provide a calendar slug, it will automatically resolve to the API ID.

        Examples:
        - `[p]luma groups addsub "Weekly Events" genai-ny` (by slug)
        - `[p]luma groups addsub "Weekly Events" cal-r8BcsXhhHYmA3tp` (by API ID)
        """
        channel_groups = await self.config.guild(ctx.guild).channel_groups()
        subscriptions = await self.config.guild(ctx.guild).subscriptions()

        if group_name not in channel_groups:
            await ctx.send(f"No channel group found named `{group_name}`.")
            return

        group = ChannelGroup.from_dict(channel_groups[group_name])

        # Enhanced subscription lookup: check both API IDs and slugs
        subscription_api_id = None

        # First, check if identifier is already a known API ID
        if subscription_identifier in subscriptions:
            subscription_api_id = subscription_identifier
        else:
            # Second, check if identifier matches any existing subscription's slug
            for existing_api_id, sub_data in subscriptions.items():
                subscription = Subscription.from_dict(sub_data)
                if subscription.slug == subscription_identifier:
                    subscription_api_id = existing_api_id
                    break

            # Third, if still not found, try to resolve slug to API ID via API
            if subscription_api_id is None:
                try:
                    async with LumaAPIClient() as client:
                        calendar_info = await client.get_calendar_info(
                            subscription_identifier
                        )

                    if not calendar_info:
                        await ctx.send(
                            f"No calendar found with slug `{subscription_identifier}`. "
                            f"Please check the slug is correct and try again."
                        )
                        return

                    subscription_api_id = calendar_info.get("api_id")
                    if not subscription_api_id:
                        await ctx.send(
                            f"Could not resolve slug `{subscription_identifier}` to an API ID."
                        )
                        return

                    # Check if this calendar is already subscribed
                    if subscription_api_id not in subscriptions:
                        # Auto-add the subscription if not already present
                        subscription = Subscription(
                            api_id=subscription_api_id,
                            slug=calendar_info.get("slug", subscription_identifier),
                            name=calendar_info.get("name", "Unknown Calendar"),
                            added_by=ctx.author.id,
                            added_at=datetime.now(timezone.utc).isoformat(),
                        )
                        subscriptions[subscription_api_id] = subscription.to_dict()
                        await self.config.guild(ctx.guild).subscriptions.set(
                            subscriptions
                        )

                        log.info(
                            f"Auto-added subscription for calendar '{subscription.name}' "
                            f"(slug: {subscription_identifier}) while adding to group '{group_name}'"
                        )

                        embed = discord.Embed(
                            title="Subscription Auto-Added",
                            description=f"Auto-added and added **{subscription.name}** to group **{group_name}**",
                            color=discord.Color.green(),
                        )
                        embed.add_field(
                            name="Slug",
                            value=f"`{subscription_identifier}`",
                            inline=True,
                        )
                        embed.add_field(
                            name="API ID", value=f"`{subscription_api_id}`", inline=True
                        )
                        embed.add_field(
                            name="Name", value=f"`{subscription.name}`", inline=True
                        )
                        await ctx.send(embed=embed)
                    else:
                        # Subscription exists, just add to group
                        subscription = Subscription.from_dict(
                            subscriptions[subscription_api_id]
                        )
                        embed = discord.Embed(
                            title="Subscription Added to Group",
                            description=f"Added **{subscription.name}** to group **{group_name}**",
                            color=discord.Color.green(),
                        )
                        embed.add_field(
                            name="Slug",
                            value=f"`{subscription_identifier}`",
                            inline=True,
                        )
                        embed.add_field(
                            name="Resolved API ID",
                            value=f"`{subscription_api_id}`",
                            inline=True,
                        )
                        await ctx.send(embed=embed)

                except LumaAPINotFoundError:
                    await ctx.send(
                        f"Calendar with slug `{subscription_identifier}` was not found. "
                        f"Please verify the slug is correct."
                    )
                    return
                except LumaAPIRateLimitError:
                    await ctx.send(
                        "API rate limit exceeded. Please wait a moment and try again."
                    )
                    return
                except LumaAPIError as e:
                    await ctx.send(
                        f"Failed to resolve calendar slug `{subscription_identifier}`: {str(e)}"
                    )
                    return
                except Exception as e:
                    log.error(f"Unexpected error resolving calendar slug: {e}")
                    await ctx.send(
                        f"An unexpected error occurred while resolving the calendar slug."
                    )
                    return

        # At this point, we have a valid subscription_api_id
        if subscription_api_id in group.subscription_ids:
            subscription = Subscription.from_dict(subscriptions[subscription_api_id])
            await ctx.send(
                f"Subscription `{subscription.name}` is already in group `{group_name}`."
            )
            return

        # Add the subscription to the group
        group.subscription_ids.append(subscription_api_id)
        channel_groups[group_name] = group.to_dict()
        await self.config.guild(ctx.guild).channel_groups.set(channel_groups)

        if subscription_identifier not in subscriptions:
            # This case handles when we already resolved a slug to API ID
            subscription = Subscription.from_dict(subscriptions[subscription_api_id])
            embed = discord.Embed(
                title="Subscription Added to Group",
                description=f"Added **{subscription.name}** to group **{group_name}**",
                color=discord.Color.green(),
            )
            embed.add_field(
                name="Identifier", value=f"`{subscription_identifier}`", inline=True
            )
            embed.add_field(
                name="API ID", value=f"`{subscription_api_id}`", inline=True
            )
            await ctx.send(embed=embed)

    @groups_group.command(name="removesub", aliases=["removesubscription"])
    @checks.admin_or_permissions(manage_guild=True)
    async def remove_subscription_from_group(
        self, ctx: commands.Context, group_name: str, subscription_api_id: str
    ):
        """Remove a subscription from a channel group."""
        channel_groups = await self.config.guild(ctx.guild).channel_groups()

        if group_name not in channel_groups:
            await ctx.send(f"No channel group found named `{group_name}`.")
            return

        group = ChannelGroup.from_dict(channel_groups[group_name])

        if subscription_api_id not in group.subscription_ids:
            await ctx.send(
                f"Subscription `{subscription_api_id}` is not in group `{group_name}`."
            )
            return

        group.subscription_ids.remove(subscription_api_id)
        channel_groups[group_name] = group.to_dict()
        await self.config.guild(ctx.guild).channel_groups.set(channel_groups)

        embed = discord.Embed(
            title="Subscription Removed from Group",
            description=f"Removed subscription from group **{group_name}**",
            color=discord.Color.red(),
        )
        await ctx.send(embed=embed)

    @groups_group.command(name="delete", aliases=["remove", "del"])
    @checks.admin_or_permissions(manage_guild=True)
    async def delete_group(self, ctx: commands.Context, *, group_name: str):
        """Delete a channel group.

        This will permanently remove a channel group and all its configuration.
        The group will no longer receive event updates.

        Parameters:
        - group_name: Name of the group to delete

        Examples:
        - `[p]luma groups delete "Weekly Events"` - Delete the "Weekly Events" group
        - `[p]luma groups remove "Weekly Events"` - Alternative alias
        """
        channel_groups = await self.config.guild(ctx.guild).channel_groups()

        if group_name not in channel_groups:
            await ctx.send(f"No channel group found named `{group_name}`.")
            return

        group = ChannelGroup.from_dict(channel_groups[group_name])
        channel = ctx.guild.get_channel(group.channel_id)
        channel_name = (
            f"#{channel.name}" if channel else f"Unknown Channel ({group.channel_id})"
        )

        # Show confirmation dialog
        embed = discord.Embed(
            title="⚠️ Delete Channel Group",
            description=f"This will permanently delete the channel group **{group_name}** including:\n"
            f"• All group configuration\n"
            f"• All subscription associations\n"
            f"• Event display settings\n\n"
            f"**Channel:** {channel_name}\n"
            f"**Subscriptions:** {len(group.subscription_ids)}\n"
            f"**Max Events:** {group.max_events}\n\n"
            "**This action cannot be undone.**",
            color=discord.Color.red(),
        )
        embed.add_field(
            name="Confirmation Required",
            value="React with ✅ to confirm or ❌ to cancel.",
            inline=False,
        )

        message = await ctx.send(embed=embed)
        await message.add_reaction("✅")
        await message.add_reaction("❌")

        def check(reaction, user):
            return (
                user == ctx.author
                and reaction.message.id == message.id
                and reaction.emoji in ["✅", "❌"]
            )

        try:
            reaction, user = await self.bot.wait_for(
                "reaction_add", timeout=60.0, check=check
            )

            if reaction.emoji == "✅":
                # Remove the group from configuration
                del channel_groups[group_name]
                await self.config.guild(ctx.guild).channel_groups.set(channel_groups)

                embed.title = "✅ Group Deleted"
                embed.description = (
                    f"Channel group **{group_name}** has been permanently deleted."
                )
                embed.color = discord.Color.green()
                await message.edit(embed=embed)

                log.info(
                    f"User {ctx.author.id} deleted channel group '{group_name}' "
                    f"from guild {ctx.guild.id}"
                )

            else:
                embed.title = "❌ Deletion Cancelled"
                embed.description = (
                    "Group deletion was cancelled. No changes were made."
                )
                embed.color = discord.Color.blue()
                await message.edit(embed=embed)

        except asyncio.TimeoutError:
            embed.title = "⏰ Deletion Cancelled"
            embed.description = "Confirmation timed out. No changes were made."
            embed.color = discord.Color.orange()
            await message.edit(embed=embed)

    @luma_group.group(name="config")
    @checks.admin_or_permissions(manage_guild=True)
    async def config_group(self, ctx: commands.Context):
        """Configure Luma plugin settings."""
        if ctx.invoked_subcommand is None:
            enabled = await self.config.guild(ctx.guild).enabled()
            interval = await self.config.update_interval_hours()
            last_update = await self.config.last_update()

            embed = discord.Embed(
                title="Luma Configuration", color=discord.Color.blue()
            )
            embed.add_field(
                name="Update Interval",
                value=f"{interval} hours (global)",
                inline=True,
            )
            embed.add_field(
                name="Enabled",
                value="Yes" if enabled else "No",
                inline=True,
            )
            embed.add_field(
                name="Last Update",
                value=last_update or "Never",
                inline=True,
            )

            await ctx.send(embed=embed)

    @config_group.command(name="interval")
    @checks.admin_or_permissions(manage_guild=True)
    async def set_update_interval(self, ctx: commands.Context, hours: int):
        """Set the update interval in hours (1-168)."""
        if not 1 <= hours <= 168:
            await ctx.send("Update interval must be between 1 and 168 hours.")
            return

        await self.config.update_interval_hours.set(hours)

        # Restart the update task with new interval
        await self.start_update_task()

        embed = discord.Embed(
            title="Update Interval Updated",
            description=f"Event updates will now occur every {hours} hours.",
            color=discord.Color.green(),
        )
        await ctx.send(embed=embed)

    @config_group.command(name="enable")
    @checks.admin_or_permissions(manage_guild=True)
    async def enable_updates(self, ctx: commands.Context):
        """Enable automatic event updates for this guild."""
        await self.config.guild(ctx.guild).enabled.set(True)

        embed = discord.Embed(
            title="Updates Enabled",
            description="Automatic event updates are now enabled for this guild.",
            color=discord.Color.green(),
        )
        await ctx.send(embed=embed)

    @config_group.command(name="disable")
    @checks.admin_or_permissions(manage_guild=True)
    async def disable_updates(self, ctx: commands.Context):
        """Disable automatic event updates for this guild."""
        await self.config.guild(ctx.guild).enabled.set(False)

        embed = discord.Embed(
            title="Updates Disabled",
            description="Automatic event updates are now disabled for this guild.",
            color=discord.Color.red(),
        )
        await ctx.send(embed=embed)

    @luma_group.command(name="update")
    @checks.admin_or_permissions(manage_guild=True)
    async def manual_update(self, ctx: commands.Context, force: Optional[str] = None):
        """Manually trigger an event update for this guild.

        This sends individual new event messages to configured channels,
        mimicking the automatic update behavior but triggered on-demand.

        Usage:
        - `[p]luma update` - Check for new events only
        - `[p]luma update force` - Send ALL recent events regardless of new status

        Parameters:
        - force: Use 'force' to send all events (useful for testing)
        """
        embed = discord.Embed(
            title="Manual Update",
            description="🔄 Checking for new events...",
            color=discord.Color.blue(),
        )
        message = await ctx.send(embed=embed)

        try:
            # Determine if force mode is enabled
            force_mode = force is not None and force.lower() in [
                "force",
                "true",
                "1",
                "yes",
            ]

            # Determine check_for_changes based on force parameter
            check_for_changes = not force_mode

            if force_mode:
                embed.description = (
                    "🔄 Force mode: Sending ALL recent events to channels..."
                )
            else:
                embed.description = "🔄 Checking for new events..."
            await message.edit(embed=embed)

            subscriptions = await self.config.guild(ctx.guild).subscriptions()
            channel_groups = await self.config.guild(ctx.guild).channel_groups()

            total_events_sent = 0

            for group_name, group_data in channel_groups.items():
                group = ChannelGroup.from_dict(group_data)
                result = await self.fetch_events_for_group(
                    group, subscriptions, check_for_changes=check_for_changes
                )

                # Send events - in force mode, send all events regardless of new status
                if result["events"]:
                    if force_mode:
                        log.info(
                            f"Force mode: Sending {len(result['events'])} events to group '{group_name}' (all events)"
                        )
                        await self.send_events_to_channel(
                            group.channel_id, result["events"], ctx.guild, group_name
                        )
                        total_events_sent += len(result["events"])
                    else:
                        # Normal mode: only send if there are new events
                        if result["new_events_count"] > 0:
                            log.info(
                                f"Manual update: Sending {result['new_events_count']} new events to group '{group_name}'"
                            )
                            await self.send_events_to_channel(
                                group.channel_id,
                                result["events"],
                                ctx.guild,
                                group_name,
                                skip_already_sent=True,
                            )
                            total_events_sent += result["new_events_count"]

            if total_events_sent > 0:
                if force_mode:
                    embed.description = f"✅ Force mode: Sent **{total_events_sent}** event(s) to channels!"
                else:
                    embed.description = f"✅ Found and sent **{total_events_sent}** new event(s) to channels!"
                embed.color = discord.Color.green()
            else:
                if force_mode:
                    embed.description = (
                        "✅ Force mode complete. No events found to send."
                    )
                else:
                    embed.description = "✅ Update complete. No new events detected."
                embed.color = discord.Color.green()

            await message.edit(embed=embed)

        except Exception as e:
            log.error(f"Manual update failed: {e}")
            embed.description = f"❌ Manual update failed: {str(e)}"
            embed.color = discord.Color.red()
            await message.edit(embed=embed)

    @luma_group.command(name="test")
    @checks.admin_or_permissions(manage_guild=True)
    async def test_subscription(self, ctx: commands.Context, subscription_api_id: str):
        """Test a subscription by fetching events directly from the API."""
        subscriptions = await self.config.guild(ctx.guild).subscriptions()

        if subscription_api_id not in subscriptions:
            await ctx.send(
                f"No subscription found with API ID `{subscription_api_id}`."
            )
            return

        subscription = Subscription.from_dict(subscriptions[subscription_api_id])

        embed = discord.Embed(
            title="Testing Subscription",
            description=f"Testing subscription: **{subscription.name}**",
            color=discord.Color.blue(),
        )
        message = await ctx.send(embed=embed)

        try:
            result = await self.fetch_events_from_subscription(subscription)

            if result["events"]:
                embed.title = "✅ Test Successful"
                embed.description = f"Successfully fetched {len(result['events'])} events from **{subscription.name}**"
                embed.color = discord.Color.green()

                # Show first few events
                for i, event in enumerate(result["events"][:3]):
                    start_time = datetime.fromisoformat(
                        event.start_at.replace("Z", "+00:00")
                    )
                    time_str = start_time.strftime("%Y-%m-%d %H:%M UTC")
                    embed.add_field(
                        name=f"{i+1}. {event.name}",
                        value=f"🕐 {time_str}\n🔗 {event.url}",
                        inline=False,
                    )

                if len(result["events"]) > 3:
                    embed.add_field(
                        name="And more...",
                        value=f"{len(result['events']) - 3} more events available",
                        inline=False,
                    )
            else:
                embed.title = "⚠️ No Events Found"
                embed.description = (
                    f"No upcoming events found for **{subscription.name}**"
                )
                embed.color = discord.Color.orange()

            await message.edit(embed=embed)

        except Exception as e:
            log.error(f"Test failed for subscription {subscription_api_id}: {e}")
            embed.title = "❌ Test Failed"
            embed.description = f"Failed to fetch events: {str(e)}"
            embed.color = discord.Color.red()
            await message.edit(embed=embed)

    @luma_group.command(name="cache")
    @checks.admin_or_permissions(manage_guild=True)
    async def cache_info(self, ctx: commands.Context):
        """Display API cache statistics.

        This command shows information about the API request caching system
        used to optimize performance and respect rate limits.

        Example:
        `[p]luma cache` - View cache statistics
        """
        embed = discord.Embed(title="API Cache Statistics", color=discord.Color.blue())

        # This would need access to the client instance
        # For now, provide basic info
        embed.add_field(name="Cache TTL", value="5 minutes", inline=True)
        embed.add_field(name="Auto-cleanup", value="Enabled", inline=True)
        embed.add_field(name="Rate Limiting", value="1 second delay", inline=True)

        await ctx.send(embed=embed)

    @luma_group.command(name="reset")
    @checks.admin_or_permissions(manage_guild=True)
    async def reset_data(self, ctx: commands.Context):
        """Delete all Luma data for this server.

        This command will remove all subscriptions, channel groups, and configuration
        for this Discord server. This action cannot be undone.

        Use this command if you want to completely remove all data stored by this cog
        for GDPR compliance or if you want to start fresh with the configuration.

        Example:
        `[p]luma reset` - Delete all Luma data for this server
        """
        embed = discord.Embed(
            title="⚠️ Reset All Data",
            description="This will permanently delete ALL Luma data for this server including:\n"
            "• All calendar subscriptions\n"
            "• All channel groups\n"
            "• All configuration settings\n\n"
            "**This action cannot be undone.**",
            color=discord.Color.red(),
        )
        embed.add_field(
            name="Confirmation Required",
            value="React with ✅ to confirm or ❌ to cancel.",
            inline=False,
        )

        message = await ctx.send(embed=embed)
        await message.add_reaction("✅")
        await message.add_reaction("❌")

        def check(reaction, user):
            return (
                user == ctx.author
                and reaction.message.id == message.id
                and reaction.emoji in ["✅", "❌"]
            )

        try:
            reaction, user = await self.bot.wait_for(
                "reaction_add", timeout=60.0, check=check
            )

            if reaction.emoji == "✅":
                # Clear all configuration
                await self.config.guild(ctx.guild).clear()

                embed.title = "✅ Data Reset Complete"
                embed.description = (
                    "All Luma data has been permanently deleted for this server."
                )
                embed.color = discord.Color.green()
                await message.edit(embed=embed)

                log.info(
                    f"Luma data reset for guild {ctx.guild.id} by user {ctx.author.id}"
                )

            else:
                embed.title = "❌ Reset Cancelled"
                embed.description = "Data reset was cancelled. No changes were made."
                embed.color = discord.Color.blue()
                await message.edit(embed=embed)

        except asyncio.TimeoutError:
            embed.title = "⏰ Reset Cancelled"
            embed.description = "Confirmation timed out. No changes were made."
            embed.color = discord.Color.orange()
            await message.edit(embed=embed)

    @luma_group.command(name="schedule", aliases=["next"])
    @commands.guild_only()
    async def show_schedule(self, ctx: commands.Context):
        """
        Show when the next automatic event update will occur and scheduling information.

        This command displays:
        • Next automatic update time - When the background task will check for new events
        • Current update interval - How often updates run (default 24 hours)
        • Manual update option - How to trigger immediate updates
        • Update status - When the last update occurred

        Examples:
        `[p]luma schedule` - View update schedule and timing information
        `[p]luma next` - Quick view of next update time
        """
        # Get current configuration
        update_interval_hours = await self.config.update_interval_hours()
        last_update_str = await self.config.last_update()
        updates_enabled = await self.config.guild(ctx.guild).enabled()

        # Calculate next update time
        now = datetime.now(timezone.utc)
        next_update_time = None
        time_until_next = None

        if last_update_str:
            try:
                last_update = datetime.fromisoformat(
                    last_update_str.replace("Z", "+00:00")
                )
                next_update_time = last_update + timedelta(hours=update_interval_hours)
                time_until_next = next_update_time - now

                # If the next update time has already passed, calculate the next one
                if next_update_time <= now:
                    # Calculate how many intervals have passed since last update
                    intervals_passed = int(
                        (now - last_update).total_seconds()
                        / (update_interval_hours * 3600)
                    )
                    next_update_time = last_update + timedelta(
                        hours=update_interval_hours * (intervals_passed + 1)
                    )
                    time_until_next = next_update_time - now
            except Exception as e:
                log.warning(f"Error calculating next update time: {e}")

        # Format time until next update
        if time_until_next:
            if time_until_next.total_seconds() <= 0:
                time_until_str = "Update is overdue - next update will happen soon"
            else:
                days = time_until_next.days
                hours = time_until_next.seconds // 3600
                minutes = (time_until_next.seconds % 3600) // 60

                if days > 0:
                    time_until_str = f"{days}d {hours}h {minutes}m"
                elif hours > 0:
                    time_until_str = f"{hours}h {minutes}m"
                else:
                    time_until_str = f"{minutes}m"
        else:
            time_until_str = "Unknown - no previous updates recorded"

        # Format next update time
        if next_update_time:
            next_update_str = next_update_time.strftime("%Y-%m-%d %H:%M UTC")
        else:
            next_update_str = "Unknown - trigger first update manually"

        # Format last update time
        if last_update_str:
            try:
                last_update = datetime.fromisoformat(
                    last_update_str.replace("Z", "+00:00")
                )
                last_update_str_formatted = last_update.strftime("%Y-%m-%d %H:%M UTC")
                ago = now - last_update
                if ago.days > 0:
                    ago_str = f"{ago.days} days ago"
                elif ago.seconds > 3600:
                    hours_ago = ago.seconds // 3600
                    ago_str = f"{hours_ago} hours ago"
                elif ago.seconds > 60:
                    minutes_ago = ago.seconds // 60
                    ago_str = f"{minutes_ago} minutes ago"
                else:
                    ago_str = "Just now"
            except Exception:
                last_update_str_formatted = last_update_str
                ago_str = "Unknown"
        else:
            last_update_str_formatted = "Never"
            ago_str = "No updates yet"

        embed = discord.Embed(
            title="⏰ Event Update Schedule",
            description="Information about automatic event updates and message scheduling",
            color=discord.Color.blue(),
            timestamp=now,
        )

        # Next update information
        embed.add_field(
            name="🕐 Next Automatic Update",
            value=f"**Time:** {next_update_str}\n**In:** {time_until_str}",
            inline=False,
        )

        # Current configuration
        embed.add_field(
            name="⚙️ Update Configuration",
            value=f"**Interval:** Every {update_interval_hours} hour(s)\n**Status:** {'✅ Enabled' if updates_enabled else '❌ Disabled'}\n**Last Update:** {last_update_str_formatted} ({ago_str})",
            inline=False,
        )

        # How messages are sent
        embed.add_field(
            name="📨 Message Sending",
            value="• **Automatic:** New events are sent when updates run\n"
            "• **Detection:** Only NEW events trigger Discord messages\n"
            "• **Frequency:** Based on the update interval above\n"
            "• **Rate Limit:** Updates are spread out to avoid spam",
            inline=False,
        )

        # Manual update options
        embed.add_field(
            name="🔧 Manual Control",
            value="• `[p]luma update` - Force check for new events now\n"
            "• `[p]luma config interval <hours>` - Change update frequency\n"
            "• `[p]luma config enable/disable` - Toggle automatic updates",
            inline=False,
        )

        # Additional helpful info
        if not updates_enabled:
            embed.add_field(
                name="⚠️ Updates Disabled",
                value="Automatic updates are currently disabled for this server. "
                "Use `[p]luma config enable` to re-enable them.",
                inline=False,
            )

        if (
            time_until_next and time_until_next.total_seconds() <= 3600
        ):  # Less than 1 hour
            embed.add_field(
                name="🚀 Update Soon",
                value="The next update will happen within the hour. "
                "Use `[p]luma update` if you need events checked immediately.",
                inline=False,
            )

        await ctx.send(embed=embed)

    @luma_group.command(name="events")
    @commands.guild_only()
    async def events(self, ctx: commands.Context):
        """
        Display upcoming events with improved formatting and actual URLs.

        Shows events from all subscriptions with:
        - Human-readable date/time formatting
        - Actual event URLs instead of just slugs
        - Event type and timezone information
        - Better overall formatting

        Example:
        `[p]luma events` - Show upcoming events with detailed formatting
        """
        subscriptions = await self.config.guild(ctx.guild).subscriptions()

        if not subscriptions:
            await ctx.send(
                "No subscriptions configured. Use `luma subscriptions add` to add one."
            )
            return

        embed = discord.Embed(
            title="📅 Upcoming Events",
            description="Here's what we have coming up:",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc),
        )

        try:
            # Step 1: Fetch from API and populate database for consistency
            all_events_data = []
            seen_api_ids = (
                set()
            )  # Track seen api_ids to prevent cross-subscription duplicates

            async with LumaAPIClient() as client:
                for sub_id, sub_data in subscriptions.items():
                    subscription = Subscription.from_dict(sub_data)
                    try:
                        # Fetch events from API
                        api_events = await client.get_calendar_events(
                            calendar_identifier=subscription.api_id,
                            limit=20,  # Fetch more events for manual display
                        )

                        # Convert to dict format for database operations
                        event_dicts = []
                        for event in api_events:
                            event_dict = {
                                "api_id": event.api_id,
                                "calendar_api_id": subscription.api_id,
                                "name": event.name,
                                "start_at": event.start_at,
                                "end_at": event.end_at,
                                "timezone": event.timezone,
                                # "event_type": event.event_type,
                                "url": event.url,
                                "last_modified": datetime.now(timezone.utc).isoformat(),
                            }
                            event_dicts.append(event_dict)

                        # Populate database with fetched events (for consistency)
                        if event_dicts:
                            await self.event_db.upsert_events(
                                event_dicts, subscription.api_id
                            )
                            all_events_data.extend(event_dicts)

                    except Exception as e:
                        log.error(
                            f"Error fetching events for subscription {subscription.name}: {e}"
                        )
                        continue

            # Step 2: Get events from database for display (ensures consistency)
            # This ensures database stats match displayed events
            all_db_events = []
            for sub_id, sub_data in subscriptions.items():
                subscription = Subscription.from_dict(sub_data)
                db_events = await self.event_db.get_tracked_events(subscription.api_id)

                # Add subscription info to database events for display
                for event_data in db_events:
                    # Deduplicate events based on api_id
                    if event_data["event_api_id"] not in seen_api_ids:
                        event_data["subscription_name"] = subscription.name
                        event_data["calendar_api_id"] = subscription.api_id
                        all_db_events.append(event_data)
                        seen_api_ids.add(event_data["event_api_id"])

            if not all_db_events:
                embed.description = "No upcoming events found."
                await ctx.send(embed=embed)
                return

            # Step 3: Sort events by start time
            all_db_events.sort(key=lambda x: x["start_at"])

            # Filter out past events and limit to next 30 days
            now = datetime.now(timezone.utc)
            cutoff_date = now + timedelta(days=30)
            upcoming_events = [
                event
                for event in all_db_events
                if now <= datetime.fromisoformat(event["start_at"].replace("Z", "+00:00")) <= cutoff_date
            ]

            if not upcoming_events:
                embed.description = "No upcoming events in the next 30 days."
                await ctx.send(embed=embed)
                return

            # Step 4: Paginate events (10 events per page/message)
            events_per_page = 10
            total_pages = (len(upcoming_events) + events_per_page - 1) // events_per_page

            for page_num in range(total_pages):
                start_idx = page_num * events_per_page
                end_idx = start_idx + events_per_page
                page_events = upcoming_events[start_idx:end_idx]

                if page_num == 0:
                    page_embed = embed
                else:
                    page_embed = discord.Embed(
                        title=f"📅 Upcoming Events (page {page_num + 1}/{total_pages})",
                        color=discord.Color.blue(),
                        timestamp=datetime.now(timezone.utc),
                    )

                for event in page_events:
                    try:
                        start_time = datetime.fromisoformat(
                            event["start_at"].replace("Z", "+00:00")
                        )
                        end_time = (
                            datetime.fromisoformat(event["end_at"].replace("Z", "+00:00"))
                            if event["end_at"]
                            else None
                        )

                        date_str = start_time.strftime("%A, %B %d, %Y")

                        local_time_str = format_local_time(
                            event["start_at"],
                            event["timezone"] or "UTC",
                            include_end_time=bool(event["end_at"]),
                            end_time_str=event["end_at"],
                        )

                        event_title = f"**{event['name']}**"

                        subscription_obj = None
                        for sub_id, sub_data in subscriptions.items():
                            sub = Subscription.from_dict(sub_data)
                            if sub.api_id == event["calendar_api_id"]:
                                subscription_obj = sub
                                break

                        if subscription_obj and subscription_obj.slug:
                            subscription_url = f"https://lu.ma/{subscription_obj.slug}"
                            event_title += f"\n*from* [{event['subscription_name']}](<{subscription_url}>)"
                        elif subscription_obj:
                            event_title += f"\n*from {event['subscription_name']}*"
                        else:
                            event_title += f"\n*from {event['subscription_name']}*"

                        details = f"📅 {date_str}\n🕐 Local Time: {local_time_str}"

                        if event["url"]:
                            event_url = f"https://lu.ma/{event['url']}"
                            details += f"\n🔗 [View Event](<{event_url}>)"

                        page_embed.add_field(
                            name=event_title,
                            value=details,
                            inline=False,
                        )

                    except Exception as e:
                        log.warning(f"Error formatting event {event['name']}: {e}")
                        continue

                await ctx.send(embed=page_embed)

        except Exception as e:
            log.error(f"Error in events command: {e}")
            await ctx.send(f"❌ Failed to fetch events: {str(e)}")

    @luma_group.group(name="database")
    async def database_group(self, ctx: commands.Context):
        """Database management commands for tracking events and viewing statistics."""
        if ctx.invoked_subcommand is None:
            embed = discord.Embed(
                title="Database Commands",
                description="Manage event tracking database and view statistics",
                color=discord.Color.blue(),
            )
            embed.add_field(
                name="Commands",
                value="• `clear` - Clear event tracking database\n"
                "• `stats` - Show database statistics",
                inline=False,
            )
            await ctx.send(embed=embed)

    @database_group.command(name="clear", aliases=["reset"])
    @checks.admin_or_permissions(manage_guild=True)
    async def clear_events_database(
        self, ctx: commands.Context, group_name: Optional[str] = None
    ):
        """Clear the event tracking database to enable resending notifications.

        This command clears only the event tracking database that prevents
        duplicate notifications. It preserves all your configuration including:
        • All calendar subscriptions
        • All channel groups
        • All configuration settings

        After clearing, all events will be treated as new and can be resent.
        This is useful for testing or when you want to resend notifications
        for events that were previously sent.

        Parameters:
        - group_name: Optional name of a channel group to clear events for only that group.
                     If not specified, clears all events globally.

        Examples:
        `[p]luma database clear` - Clear all event tracking data
        `[p]luma database clear "Weekly Events"` - Clear events for a specific group only
        `[p]luma database clear "Group Name"` - Clear events for the named group
        """
        # Get channel groups for validation
        channel_groups = await self.config.guild(ctx.guild).channel_groups()
        subscriptions = await self.config.guild(ctx.guild).subscriptions()

        # If group_name is specified, validate it exists and has subscriptions
        if group_name:
            if group_name not in channel_groups:
                await ctx.send(
                    f"❌ Channel group `{group_name}` not found. "
                    f"Available groups: {', '.join(channel_groups.keys()) or 'None'}"
                )
                return

            group = ChannelGroup.from_dict(channel_groups[group_name])
            if not group.subscription_ids:
                await ctx.send(
                    f"⚠️ Channel group `{group_name}` has no subscriptions. "
                    "Nothing to clear."
                )
                return

            # Show which calendars will be affected
            affected_calendars = []
            for sub_id in group.subscription_ids:
                if sub_id in subscriptions:
                    sub = Subscription.from_dict(subscriptions[sub_id])
                    affected_calendars.append(f"{sub.name} ({sub.api_id[:12]}...)")

            if not affected_calendars:
                await ctx.send(
                    f"⚠️ No valid subscriptions found for group `{group_name}`."
                )
                return

            description = f"This will clear event tracking data for **group: {group_name}** including:\n"
            for calendar in affected_calendars:
                description += f"• {calendar}\n"
            description += f"\n**PRESERVED:**\n• All other groups and their events\n• All configuration settings\n\n**After clearing, events from this group will be treated as new.**"

        else:
            # Global clear - get current database stats for confirmation
            try:
                stats = await self.event_db.get_calendar_stats()
                total_events = stats.get("total_events", 0)
                total_sends = stats.get("total_sends", 0)
            except Exception:
                total_events = 0
                total_sends = 0

            description = "This will clear the event tracking database including:\n"
            description += f"• {total_events} tracked events\n"
            description += f"• {total_sends} send history records\n\n"
            description += "**PRESERVED:**\n• All calendar subscriptions\n• All channel groups\n• All configuration settings\n\n**After clearing, all events will be treated as new.**"

        embed = discord.Embed(
            title="⚠️ Clear Event Database",
            description=description,
            color=discord.Color.orange(),
        )
        clear_type = (
            f"**for group: {group_name}**" if group_name else "**GLOBAL CLEAR**"
        )
        embed.add_field(
            name="Scope",
            value=clear_type,
            inline=False,
        )
        embed.add_field(
            name="Confirmation Required",
            value="React with ✅ to confirm or ❌ to cancel.",
            inline=False,
        )

        message = await ctx.send(embed=embed)
        await message.add_reaction("✅")
        await message.add_reaction("❌")

        def check(reaction, user):
            return (
                user == ctx.author
                and reaction.message.id == message.id
                and reaction.emoji in ["✅", "❌"]
            )

        try:
            reaction, user = await self.bot.wait_for(
                "reaction_add", timeout=60.0, check=check
            )

            if reaction.emoji == "✅":
                try:
                    # Clear the event database
                    if group_name:
                        # Group-specific clear
                        log.info(
                            f"Starting group-specific clear for group '{group_name}' in guild {ctx.guild.id}"
                        )

                        # Get calendar IDs for this group
                        calendar_ids = await self.event_db.get_calendars_for_group(
                            group_name, channel_groups
                        )

                        if not calendar_ids:
                            embed.title = "❌ No Calendars Found"
                            embed.description = (
                                f"No calendars found for group `{group_name}`."
                            )
                            embed.color = discord.Color.red()
                            await message.edit(embed=embed)
                            return

                        result = await self.event_db.clear_event_database(calendar_ids)
                    else:
                        # Global clear
                        log.info(
                            f"Starting global database clear for guild {ctx.guild.id}"
                        )
                        result = await self.event_db.clear_event_database()

                    if result["success"]:
                        if group_name:
                            embed.title = "✅ Group Database Cleared"
                            embed.description = (
                                f"Successfully cleared event tracking data for **group: {group_name}**:\n"
                                f"• {result['events_cleared']} events cleared\n"
                                f"• {result['history_cleared']} history records cleared\n\n"
                                f"Events from this group will now be treated as new."
                            )
                        else:
                            embed.title = "✅ Global Database Cleared"
                            embed.description = (
                                f"Successfully cleared event tracking database:\n"
                                f"• {result['events_cleared']} events cleared\n"
                                f"• {result['history_cleared']} history records cleared\n\n"
                                f"All events will now be treated as new and can be resent."
                            )
                        embed.color = discord.Color.green()

                        log.info(
                            f"User {ctx.author.id} cleared {'group-specific' if group_name else 'global'} "
                            f"event database for guild {ctx.guild.id}: {result['events_cleared']} events, "
                            f"{result['history_cleared']} history records"
                        )
                    else:
                        embed.title = "❌ Failed to Clear Database"
                        embed.description = f"Failed to clear event database: {result.get('error', 'Unknown error')}"
                        embed.color = discord.Color.red()

                except Exception as e:
                    log.error(f"Failed to clear event database: {e}")
                    embed.title = "❌ Error"
                    embed.description = (
                        f"An error occurred while clearing the database: {str(e)}"
                    )
                    embed.color = discord.Color.red()

                await message.edit(embed=embed)

            else:
                embed.title = "❌ Clear Cancelled"
                embed.description = (
                    "Event database clear was cancelled. No changes were made."
                )
                embed.color = discord.Color.blue()
                await message.edit(embed=embed)

        except asyncio.TimeoutError:
            embed.title = "⏰ Clear Cancelled"
            embed.description = "Confirmation timed out. No changes were made."
            embed.color = discord.Color.orange()
            await message.edit(embed=embed)

    @database_group.command(name="stats")
    @checks.admin_or_permissions(manage_guild=True)
    async def event_database_stats(self, ctx: commands.Context):
        """Show event database statistics and tracking information.

        Displays information about:
        • Total tracked events across all calendars
        • Number of calendars being tracked
        • Total message sends recorded
        • Per-calendar event counts

        Example:
        `[p]luma database stats` - View database statistics
        """
        try:
            stats = await self.event_db.get_calendar_stats()

            embed = discord.Embed(
                title="📊 Event Database Statistics",
                description="Current event tracking database status:",
                color=discord.Color.blue(),
            )

            # Add overall stats
            total_events = stats.get("total_events", 0)
            total_calendars = stats.get("total_calendars", 0)
            total_sends = stats.get("total_sends", 0)

            embed.add_field(
                name="Overall Statistics",
                value=f"📅 **Total Events:** {total_events}\n"
                f"📆 **Calendars Tracked:** {total_calendars}\n"
                f"📨 **Messages Sent:** {total_sends}",
                inline=False,
            )

            # Add per-calendar stats
            calendar_stats = stats.get("calendar_stats", [])
            if calendar_stats:
                calendar_info = []
                for cal_stat in calendar_stats[:5]:  # Limit to 5 calendars
                    calendar_info.append(
                        f"📋 `{cal_stat['calendar_api_id'][:12]}...` - {cal_stat['event_count']} events"
                    )

                if len(calendar_stats) > 5:
                    calendar_info.append(
                        f"... and {len(calendar_stats) - 5} more calendars"
                    )

                embed.add_field(
                    name="Calendar Breakdown",
                    value="\n".join(calendar_info),
                    inline=False,
                )
            else:
                embed.add_field(
                    name="Calendar Breakdown",
                    value="No calendars currently tracked",
                    inline=False,
                )

            # Add helpful info
            embed.add_field(
                name="What This Means",
                value="• **Events:** Unique events stored in database\n"
                "• **Calendars:** Different calendars being monitored\n"
                "• **Messages:** Total notifications sent to channels\n\n"
                "Use `[p]luma database clear` to reset tracking data.",
                inline=False,
            )

            await ctx.send(embed=embed)

        except Exception as e:
            log.error(f"Failed to get database stats: {e}")
            embed = discord.Embed(
                title="❌ Error",
                description=f"Failed to get database statistics: {str(e)}",
                color=discord.Color.red(),
            )
            await ctx.send(embed=embed)
