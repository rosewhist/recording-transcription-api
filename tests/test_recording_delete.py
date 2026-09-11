from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_delete_recording_returns_204(
    client: AsyncClient,
    mock_repo: MagicMock,
):
    recording_id = uuid4()
    file_path = "/tmp/demo.mp3"
    mock_repo.delete = AsyncMock(return_value=file_path)

    with patch(
        "api.service.recording.file_utils.remove_file_async",
        new_callable=AsyncMock,
    ) as remove_file:
        resp = await client.delete(f"/v1/recordings/{recording_id}")

    assert resp.status_code == 204
    assert resp.content == b""
    mock_repo.delete.assert_awaited_once_with(recording_id)
    remove_file.assert_awaited_once_with(file_path)


@pytest.mark.asyncio
async def test_delete_recording_not_found(
    client: AsyncClient,
    mock_repo: MagicMock,
):
    recording_id = uuid4()
    mock_repo.delete = AsyncMock(return_value=None)

    with patch(
        "api.service.recording.file_utils.remove_file_async",
        new_callable=AsyncMock,
    ) as remove_file:
        resp = await client.delete(f"/v1/recordings/{recording_id}")

    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "recording_not_found"
    remove_file.assert_not_called()
