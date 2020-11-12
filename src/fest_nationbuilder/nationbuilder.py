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

        self.nationbuilder_base_url = f"https://{nation_slug}.nationbuilder.com"

    def get_events(self, **kwargs):
        return utils.Future(self.iter_events(**kwargs))

    def iter_events(self, path=None, **kwargs):
        if not path:
            path = f"/api/v1/sites/{self.site_slug}/pages/events"
        kwargs["calendar_id"] = self.calendar_id

        url = f"{self.nationbuilder_base_url}{path}"
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

    def callbackgen(self, verb):
        """ Generate callback function to collect responses. """

        # def callback(_, res, err):
        #     if err:
        #         raise err
        #     facebook_id = res["extendedProperties"]["private"]["facebookId"]
        #     self.responses[verb][facebook_id] = res

        # return callback

    def batchgen(self, method, verb):
        """ Generate batched requests with callback. """
        # requests = self.requests[verb]
        # if any(requests):
        #     count = 0
        #     batch = self.calendar.batch(self.callbackgen(verb))
        #     for req in requests.values():
        #         self.calendar.logger.info(
        #             "%s /%s/events/%s", verb, req["calendarId"], req.get("eventId", "")
        #         )
        #         batch.add(method(**req))
        #         count += 1
        #         if count == MAX_BATCH_REQUESTS:
        #             count = 0
        #             yield batch
        #             batch = self.calendar.batch(self.callbackgen(verb))
        #     yield batch

    def execbatch(self, method, verb, dryrun=False):
        """ Execute batches. """
        # batches = self.batchgen(method, verb)
        # for batch in batches:
        #     if dryrun:
        #         for req in batch._requests.values():
        #             body = json.loads(req.body)
        #             fid = body["extendedProperties"]["private"]["facebookId"]
        #             self.responses[verb][fid] = body
        #     else:
        #         batch.execute()

    def execute(self, dryrun=True):
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
                str.removeprefix(t, EVENT_ID_PREFIX)
                for t in x["tags"]
                if str.startswith(EVENT_ID_PREFIX)
            ): {
                "digest": next(
                    str.removeprefix(t, DIGEST_PREFIX)
                    for t in x["tags"]
                    if str.startswith(DIGEST_PREFIX)
                ),
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
            if "gcal_id:" + self.gcal.calendar_id in x["tags"]
        }

        # Get create/update/delete request payloads
        for gcal_id, event in gcal_events.items():
            digest = utils.digest(event)
            if gcal_id not in nb_events:
                self.requests["POST"][gcal_id] = {
                    "body": gcal_to_nb(event, self.gcal.calendar_id),
                }

            elif digest != nb_events[gcal_id]["digest"]:
                self.requests["PUT"][gcal_id] = {
                    "id": nb_events[gcal_id]["nationbuilder_id"],
                    "body": gcal_to_nb(event, self.gcal.calendar_id),
                }

        found_ids = {x["id"] for x in nb_events}

        for gcal_id in nb_events.keys() - found_ids:
            self.requests["DELETE"][gcal_id] = {
                "id": nb_events[gcal_id]["nationbuilder_id"]
            }

        # Execute batched requests
        print(json.dumps(self.requests, indent=4))

        # self.execbatch(some_insert_method, "POST", dryrun)
        # self.execbatch(some_update_method, "PUT", dryrun)
        # self.execbatch(some_delete_method, "DELETE", dryrun)
        # return self
