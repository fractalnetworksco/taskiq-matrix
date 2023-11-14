"""
This script is intended to be ran by the test container's entrypoint.

Ensures that the provided test user exists and creates a room for testing.
On Success, writes necessary environment variables to .env.testing.
"""

import asyncio
import os
from sys import exit

import docker
from aiofiles import open
from docker.models.containers import Container
from nio import AsyncClient, LoginError, RoomCreateError

ENV = os.environ.get("ENV", "dev")
TEST_CONFIG_DIR = os.environ.get("TEST_CONFIG_DIR", ".")
TEST_ENV_FILE = os.environ.get("TEST_ENV_FILE", f"{TEST_CONFIG_DIR}/taskiq-matrix.{ENV}.env")
TEST_HOMESERVER_URL = os.environ.get("TEST_HOMESERVER_URL", "http://localhost:8008")
TEST_USER_USERNAME = os.environ.get("TEST_USER_USERNAME", "admin")
TEST_USER_PASSWORD = os.environ.get("TEST_USER_PASSWORD", "admin")
PYTHON_BIN = os.environ.get("PYTHON_BIN", "venv/bin/python")


async def main():
    # this is blocking but doesn't matter since this is an entrypoint
    try:
        # get homeserver container
        docker_client = docker.from_env()
        synapse_container = docker_client.containers.list(
            filters={"label": "org.homeserver=true"}
        )[0]
        # asserting here so that the Container type hint works : )
        assert isinstance(synapse_container, Container)
    except Exception:
        print("No homeserver container found")
        print("Launch synapse container in /synapse")
        exit(1)

    # create admin user on synapse if it doesn't exist
    result = synapse_container.exec_run(
        f"register_new_matrix_user -c /data/homeserver.yaml -a -u {TEST_USER_USERNAME} -p {TEST_USER_PASSWORD} http://localhost:8008"
    )

    if "User ID already taken" not in result.output.decode("utf-8") and result.exit_code != 0:
        print(result.output.decode("utf-8"))
        exit(1)

    # login
    matrix_client = AsyncClient(TEST_HOMESERVER_URL, TEST_USER_USERNAME)
    login_res = await matrix_client.login(TEST_USER_PASSWORD)
    if isinstance(login_res, LoginError):
        print(f"Error logging in: {login_res.message}")
        exit(1)

    # disable rate limiting for the created test user
    print(f"Disabling rate limiting for user: {matrix_client.user_id}")
    result = synapse_container.exec_run(
        f"sqlite3 /data/homeserver.db -cmd \"INSERT INTO ratelimit_override values ('{matrix_client.user_id}', 0, 0);\" .exit"
    )
    if "UNIQUE constraint failed" not in result.output.decode("utf-8") and result.exit_code != 0:
        print(result.output.decode("utf-8"))
        exit(1)

    # restart synapse container
    print("Restarting synapse container")
    synapse_container.restart()

    print("Creating room")
    # create room
    # This always creates a new room. This is okay since we want a fresh start
    room_create_res = await matrix_client.room_create(name="Test Room")
    if isinstance(room_create_res, RoomCreateError):
        print(f"Error creating room: {room_create_res.message}")
        exit(1)

    # write environment file
    async with open(TEST_ENV_FILE, "w") as f:
        await f.write(
            f'export MATRIX_ROOM_ID="{room_create_res.room_id}"\nexport MATRIX_ACCESS_TOKEN="{matrix_client.access_token}"\nexport MATRIX_HOMESERVER_URL="{TEST_HOMESERVER_URL}"\nexport PYTHON_BIN="{PYTHON_BIN}"\n'
        )

    await matrix_client.close()

    print("Successfully prepared")


if __name__ == "__main__":
    asyncio.run(main())
