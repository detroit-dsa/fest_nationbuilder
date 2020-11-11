import datetime
import urllib
from pprint import pprint

from fest_nationbuilder.utils import gcal_event_to_nationbuilder

import requests
from fest import utils
from fest.google import GoogleCalendar
from fest.utils import Future


class NationBuilder:
    def __init__(self, api_token, nation_slug, site_slug, calendar_id):
        self.logger = utils.logger(self)
        self.api_token = api_token
        self.nation_slug = nation_slug
        self.site_slug = site_slug
        self.calendar_id = calendar_id

        self.nationbuilder_base_url = f"https://{nation_slug}.nationbuilder.com"

    def get_events(self, **kwargs):
        return utils.Future(self.iter_events(**kwargs))

    def iter_events(self, path=None, **kwargs):
        if not path:
            path = f"/api/v1/sites/{self.site_slug}/pages/events"

        url = f"{self.nationbuilder_base_url}{path}"

        kwargs["calendar_id"] = self.calendar_id
        params = urllib.parse.urlencode(kwargs)
        self.logger.info(f" GET {url}?{params}")

        kwargs["access_token"] = self.api_token
        response = requests.get(url, kwargs).json()

        items = response["results"] or []
        yield from items

        if response["next"]:
            yield from self.iter_events(response["next"], **kwargs)

    def sync(self, gcal: GoogleCalendar, **kwargs):
        return NationBuilderSyncFuture(
            gcal.get_events(singleEvents=True, **kwargs), gcal, self
        )


class NationBuilderSyncFuture:
    def __init__(
        self, gcal_get_events_future: Future, gcal: GoogleCalendar, nb: NationBuilder
    ):
        self.gcal_get_events_future = gcal_get_events_future
        self.nb = nb
        self.gcal = gcal
        self.requests = {"POST": {}, "PUT": {}, "DELETE": {}}
        self.responses = {"POST": {}, "PUT": {}, "DELETE": {}}

    def execute(self):
        # Get Google Calendar events
        gcal_events = {x["id"]: x for x in self.gcal_get_events_future.execute()}

        if not any(gcal_events):
            self.nb.logger.info("NO-OP")
            return self

        # Get NationBuilder events
        start_times = [
            x["start"]["dateTime"]
            for x in gcal_events.values()
            if "start" in x and "dateTime" in x["start"]
        ]
        end_times = [
            x["end"]["dateTime"]
            for x in gcal_events.values()
            if "end" in x and "dateTime" in x["end"]
        ]

        # address1: Google Calendar event ID
        # address2: Google Calendar event digest
        nb_events = {
            x["venue"]["address"]["address1"]: {
                "digest": x["venue"]["address"]["address2"],
                "nationbuilder_id": x["id"],
            }
            for x in self.nb.iter_events(
                starting=min(start_times + end_times),
                until=str(
                    datetime.datetime.strptime(
                        max(start_times + end_times), "%Y-%m-%dT%H:%M:%S%z"
                    )
                    + datetime.timedelta(seconds=1)
                ),
            )
            if "gcal_" + self.gcal.calendar_id in x["tags"]
        }

        # Get create/update/delete request payloads
        for gcal_id, event in gcal_events.items():
            digest = utils.digest(event)
            if gcal_id not in nb_events:
                self.requests["POST"][gcal_id] = {
                    "body": gcal_event_to_nationbuilder(event, self.gcal.calendar_id),
                }

            elif digest != nb_events[gcal_id]["digest"]:
                self.requests["PUT"][gcal_id] = {
                    "id": nb_events[gcal_id]["nationbuilder_id"],
                    "body": gcal_event_to_nationbuilder(event, self.gcal.calendar_id),
                }

        found_ids = {x["id"] for x in nb_events}

        for gcal_id in nb_events.keys() - found_ids:
            self.requests["DELETE"][gcal_id] = {
                "id": nb_events[gcal_id]["nationbuilder_id"]
            }

        # Execute batched requests
        pprint(self.requests, width=140)
