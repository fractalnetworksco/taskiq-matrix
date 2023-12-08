from typing import Any, Dict, Optional
from uuid import uuid4

from fractal.matrix.async_client import FractalAsyncClient
from nio import SyncError

EMPTY_FILTER = {
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
) -> Dict[str, Any]:
    """
    Create a filter for a room and/or specific message types.

    Returns:
        filter dict
    """
    if limit is None:
        return {
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

    return {
        "presence": {"limit": 0, "types": []},
        "account_data": {"limit": 0, "types": []},
        "room": {
            "rooms": [room_id],
            "state": {"types": [], "limit": 0},
            "timeline": {
                "types": [*types],
                "not_types": [*not_types],
                "not_senders": [*not_senders],
                "limit": limit,
            },
        },
        "request_id": str(uuid4()),
    }


async def get_sync_token(client: FractalAsyncClient) -> str:
    """
    Runs an empty sync request and returns the next_batch token.
    """
    res = await client.sync(timeout=0, sync_filter=EMPTY_FILTER, since=None)
    if isinstance(res, SyncError):
        raise Exception(res.message)
    return res.next_batch


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

    def get_content_only(event):
        content = event.source["content"]
        content["sender"] = event.sender
        return content

    if since is None:
        client.next_batch = None

    res = await client.sync(timeout=timeout, sync_filter=filter, since=since)
    if isinstance(res, SyncError):
        raise Exception(res.message)

    rooms = list(res.rooms.join.keys())
    filter_keys = kwargs.keys()
    d = {}
    for room in rooms:
        if content_only:
            d[room] = [get_content_only(event) for event in res.rooms.join[room].timeline.events]
        else:
            d[room] = [event.source for event in res.rooms.join[room].timeline.events]

    if kwargs:
        # filter out all keys by value from kwargs
        for key in filter_keys:
            d = {k: [i for i in v if i.get(key) == kwargs[key]] for k, v in d.items()}
    return d


async def get_first_unacked_task(tasks: list[Dict[str, Any]]) -> Dict[str, Any]:
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

        if task["content"]["msgtype"].startswith("taskiq.task"):
            if task_id not in task_dict:
                # add task to dictionary and initially mark it as unacknowledged
                task_dict[task_id] = {"task_data": task, "acknowledged": False}

            else:
                # task already exists in dictionary (weve seen an ack for it)
                # simply update the task's data
                task_dict[task_id]["task_data"] = task

        elif task["content"]["msgtype"].startswith("taskiq.ack"):
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
