from fest.utils import digest


def gcal_event_to_nationbuilder(event, google_calendar_id):
    """ Convert a Google Calendar event to a NationBuilder event. """
    return {
        "status": "unlisted",
        "name": event["summary"],
        "excerpt": event.get("description"),
        "start_time": event["start"]["dateTime"],
        "end_time": event["end"]["dateTime"],
        "venue": {"address": {"address1": event["id"], "address2": digest(event)}},
        "tags": ["gcal_" + google_calendar_id],
    }
