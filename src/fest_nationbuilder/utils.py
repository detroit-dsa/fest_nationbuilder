from fest.utils import digest


def gcal_to_nb(gcal_event, gcal_calendar_id, nb_calendar_id):
    """ Convert a Google Calendar event to a NationBuilder event. """
    return {
        "calendar_id": nb_calendar_id,
        "status": "unlisted",
        "name": gcal_event["summary"],
        "intro": gcal_event.get("description"),
        "start_time": gcal_event["start"]["dateTime"],
        "end_time": gcal_event["end"]["dateTime"],
        "tags": [
            "gcal_id:" + gcal_calendar_id,
            "gcal_event_id:" + gcal_event["id"],
            "gcal_event_digest:" + digest(gcal_event),
        ],
        "venue": {"name": gcal_event.get("location")},
    }
