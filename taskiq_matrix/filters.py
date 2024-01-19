from copy import deepcopy
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Union
from uuid import uuid4

if TYPE_CHECKING: # pragma: no cover
    from taskiq_matrix.matrix_queue import TaskTypes

from fractal.matrix.async_client import FractalAsyncClient
from nio import BadEvent, Event, MessageDirection, RoomMessagesError, SyncError

EMPTY_FILTER: Dict[str, Union[Dict[str, Any], str]] = {
    "presence": {"limit": 0, "types": []},
    "account_data": {"limit": 0, "types": []},
    "room": {
        "rooms": [],
        "state": {"types": [], "limit": 0},
        "timeline": {"types": [], "limit": 0},
        "account_data": {"limit": 0, "types": []},
        "ephemeral": {"limit": 0, "types": []},
    },
}

NO_ROOM_FILTER = {
    "presence": {"limit": 0, "types": []},
    "account_data": {"limit": 0, "types": []},
    "room": {"rooms": []},
}

# filters rooms with pending invites
INVITE_FILTER = {
    "presence": {"limit": 0, "types": []},
    "account_data": {"limit": 0, "types": []},
    "room": {
        "state": {"types": ["m.room.join_rules"], "not_types": ["m.room.member"], "limit": 0},
        "timeline": {"types": [], "limit": 0},
        "account_data": {"limit": 0, "types": []},
        "ephemeral": {"limit": 0, "types": []},
    },
}


def create_filter(
    room_id: str,
    types: list = [],
    not_types: list = [],
    limit: Optional[int] = None,
    not_senders: list = [],
    room_event_filter: bool = False,
) -> Dict[str, Any]:
    """
    Create a filter for a room and/or specific message types.

    Returns:
        filter dict
    """
    message_filter = {
        "presence": {"limit": 0, "types": []},
        "account_data": {"limit": 0, "types": []},
        "room": {
            "rooms": [room_id],
            "state": {"types": [], "limit": 0},
            "timeline": {
                "types": [*types],
                "not_types": [*not_types],
                "not_senders": [*not_senders],
            },
        },
        "request_id": str(uuid4()),
    }
    if limit is not None:
        message_filter["room"]["timeline"]["limit"] = limit

    if room_event_filter:
        room_filter = message_filter["room"]["timeline"]
        room_filter["request_id"] = message_filter["request_id"]
        return room_filter

    return message_filter


def create_sync_filter(
    room_id: str,
    types: list = [],
    not_types: list = [],
    limit: Optional[int] = None,
    not_senders: list = [],
):
    """
    Creates a filter that works with the sync endpoint.
    """
    return create_filter(
        room_id, types=types, not_types=not_types, limit=limit, not_senders=not_senders
    )


def create_room_message_filter(
    room_id: str,
    types: list = [],
    not_types: list = [],
    limit: Optional[int] = None,
    not_senders: list = [],
):
    """
    Creates a filter that works with the room_messages endpoint.
    """
    return create_filter(
        room_id,
        types=types,
        not_types=not_types,
        limit=limit,
        not_senders=not_senders,
        room_event_filter=True,
    )


def _get_content_only(event: Union[Event, BadEvent]):
    content = event.source["content"]
    content["sender"] = event.sender
    content["event_id"] = event.event_id
    return content


async def run_sync_filter(
    client: FractalAsyncClient,
    filter: dict,
    timeout: int = 30000,
    since: Optional[str] = None,
    content_only: bool = True,
    **kwargs,
) -> Dict[str, Any]:
    """
    Execute a filter with the provided client, optionally filter message body by kwargs
    attempts to deserialize json
    """
    if since is None:
        client.next_batch = None  # type:ignore

    res = await client.sync(timeout=timeout, sync_filter=filter, since=since)
    if isinstance(res, SyncError):
        raise Exception(res.message)

    rooms = list(res.rooms.join.keys())
    d = {}
    for room in rooms:
        if content_only:
            d[room] = [_get_content_only(event) for event in res.rooms.join[room].timeline.events]
        else:
            d[room] = [event.source for event in res.rooms.join[room].timeline.events]

    return d


async def run_room_message_filter(
    client: FractalAsyncClient,
    room_id: str,
    filter: dict,
    since: Optional[str] = None,
    content_only: bool = True,
    direction: MessageDirection = MessageDirection.front,
    limit: int = 100,
) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Execute a room message request with the provided client attempts to deserialize json
    """
    since = since or ""

    end = None

    if direction == MessageDirection.back:
        end = ""

    res = await client.room_messages(
        room_id,
        start=since,
        end=end,
        limit=limit,
        direction=direction,
        message_filter=filter,
    )
    if isinstance(res, RoomMessagesError):
        raise Exception(res.message)

    d = {}
    if res.chunk:
        if content_only:
            d[room_id] = [_get_content_only(event) for event in res.chunk]
        else:
            d[room_id] = [event.source for event in res.chunk]

    if direction == MessageDirection.back:
        return d, res.start
    else:
        return d, res.end

async def get_first_unacked_task(
    tasks: list[Dict[str, Any]], task_types: "TaskTypes"
) -> Dict[str, Any]:
    """
    Returns the first task object that has not been acknowledged.
    """
    task_dict = {}
    task_order = []

    for task in tasks:
        task_body = task["content"]["body"]
        task_id = task_body["task_id"]

        # add task id to order list if its not already there
        if task_id not in task_order:
            task_order.append(task_id)

        if task["content"]["msgtype"] == task_types.task:
            if task_id not in task_dict:
                # add task to dictionary and initially mark it as unacknowledged
                task_dict[task_id] = {"task_data": task, "acknowledged": False}

            else:
                # task already exists in dictionary (weve seen an ack for it)
                # simply update the task's data
                task_dict[task_id]["task_data"] = task

        elif task["content"]["msgtype"].startswith(task_types.ack):
            if task_id in task_dict:
                # mark the task as acknowledged if it exists in the task dictionary
                task_dict[task_id]["ack_data"] = task
                task_dict[task_id]["acknowledged"] = True

            # if the task doesnt exist in the dictionary, add it in as acknowledged
            # this handles the case where the ack is somehow ahead of the task request
            else:
                task_dict[task_id] = {"ack_data": task, "acknowledged": True}

    for task_id in task_order:
        # return the first task that has not been acknowledged
        if task_id in task_dict and not task_dict[task_id]["acknowledged"]:
            return task_dict[task_id]["task_data"]

    return {}
