import datetime
import json
import urllib

import requests
from fest import utils
from fest.google import GoogleCalendar
from fest.utils import Future

from fest_nationbuilder.utils import gcal_to_nb

DIGEST_PREFIX = "gcal_event_digest:"
EVENT_ID_PREFIX = "gcal_event_id:"


class NationBuilder:
    def __init__(self, api_token, nation_slug, site_slug, calendar_id):
        self.logger = utils.logger(self)
        self.api_token = api_token
        self.site_slug = site_slug
        self.calendar_id = calendar_id

        self.BASE_URL = f"https://{nation_slug}.nationbuilder.com"
        self.EVENTS_PATH = f"/api/v1/sites/{self.site_slug}/pages/events"

    def get_events(self, **kwargs):
        return utils.Future(self.iter_events(**kwargs))

    def iter_events(self, path=None, **kwargs):
        if not path:
            path = self.EVENTS_PATH
        kwargs["calendar_id"] = self.calendar_id

        url = f"{self.BASE_URL}{path}"
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

    def execute(self, dry_run=True):
        # Get Google Calendar events
        gcal_events = {x["id"]: x for x in self.gcal_get_events_future.execute()}

        if not any(gcal_events):
            self.nb.logger.info("NO-OP")
            return self

        # Get NationBuilder events
        start_times = [
            x["start"]["dateTime"]
            for x in gcal_events.values()
            if x.get("start", {}).get("dateTime")
        ]
        end_times = [
            x["end"]["dateTime"]
            for x in gcal_events.values()
            if x.get("end", {}).get("dateTime")
        ]

        nb_events = {
            next(
                t.removeprefix(EVENT_ID_PREFIX)
                for t in x["tags"]
                if t.startswith(EVENT_ID_PREFIX)
            ): {
                "digest": next(
                    t.removeprefix(DIGEST_PREFIX)
                    for t in x["tags"]
                    if t.startswith(DIGEST_PREFIX)
                ),
                "nb_id": x["id"],
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
            if "gcal_id:" + self.gcal.calendar_id in x["tags"]
        }

        # Get create/update/delete request payloads
        for gcal_id, event in gcal_events.items():
            digest = utils.digest(event)
            if gcal_id not in nb_events:
                self.requests["POST"][gcal_id] = {
                    "event": gcal_to_nb(event, self.gcal.calendar_id, self.nb.calendar_id),
                }

            elif digest != nb_events[gcal_id]["digest"]:
                self.requests["PUT"][gcal_id] = {
                    "id": nb_events[gcal_id]["nb_id"],
                    "event": gcal_to_nb(event, self.gcal.calendar_id, self.nb.calendar_id),
                }

        for gcal_id in nb_events.keys() - gcal_events.keys():
            self.requests["DELETE"][gcal_id] = {
                "id": nb_events[gcal_id]["nb_id"]
            }

        # Execute requests
        api_events_url = (
            f"{self.nb.BASE_URL}/api/v1/sites/{self.nb.site_slug}/pages/events"
        )

        for nb_id, req in self.requests["POST"].items():
            self.responses["POST"][nb_id] = (
                req
                if dry_run
                else requests.post(
                    api_events_url, json=req, params={"access_token": self.nb.api_token}
                )
            )

        for nb_id, req in self.requests["PUT"].items():
            self.responses["PUT"][nb_id] = (
                req
                if dry_run
                else requests.put(
                    f"{api_events_url}/{nb_id}",
                    json=req,
                    params={"access_token": self.nb.api_token},
                )
            )

        for nb_id, req in self.requests["DELETE"].items():
            self.responses["DELETE"][nb_id] = (
                req
                if dry_run
                else requests.delete(
                    f"{api_events_url}/{nb_id}",
                    params={"access_token": self.nb.api_token},
                )
            )

        return self
