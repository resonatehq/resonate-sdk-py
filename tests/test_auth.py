from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

from resonate.message_sources import Poller
from resonate.stores import RemoteStore


class TestRemoteStoreTokenAuth:
    def test_token_param_sets_bearer_header(self) -> None:
        store = RemoteStore(token="test-token-123")
        assert store._token == "test-token-123"

    def test_token_env_var_fallback(self) -> None:
        with patch.dict(os.environ, {"RESONATE_TOKEN": "env-token-456"}):
            store = RemoteStore()
            assert store._token == "env-token-456"

    def test_token_param_takes_priority_over_env_var(self) -> None:
        with patch.dict(os.environ, {"RESONATE_TOKEN": "env-token"}):
            store = RemoteStore(token="arg-token")
            assert store._token == "arg-token"

    def test_token_takes_priority_over_basic_auth(self) -> None:
        store = RemoteStore(token="my-token", auth=("user", "pass"))
        assert store._token == "my-token"
        assert store._auth == ("user", "pass")

    def test_no_token_when_not_provided(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            store = RemoteStore()
            assert store._token is None

    def test_bearer_header_applied_to_request(self) -> None:
        store = RemoteStore(token="test-token-123")
        mock_req = MagicMock()
        mock_req.headers = {}

        # Mock the session and response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"id": "test"}
        mock_response.raise_for_status = MagicMock()

        with patch("resonate.stores.remote.Session") as MockSession:
            mock_session = MagicMock()
            mock_session.send.return_value = mock_response
            MockSession.return_value.__enter__ = MagicMock(return_value=mock_session)
            MockSession.return_value.__exit__ = MagicMock(return_value=False)

            store.call(mock_req)

            # Verify Bearer token was set on the request headers
            assert mock_req.headers["Authorization"] == "Bearer test-token-123"
            # Verify prepare_auth was NOT called (token takes priority)
            mock_req.prepare_auth.assert_not_called()

    def test_basic_auth_used_when_no_token(self) -> None:
        store = RemoteStore(auth=("user", "pass"))
        mock_req = MagicMock()
        mock_req.headers = {}

        # Mock the session and response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"id": "test"}
        mock_response.raise_for_status = MagicMock()

        with patch("resonate.stores.remote.Session") as MockSession:
            mock_session = MagicMock()
            mock_session.send.return_value = mock_response
            MockSession.return_value.__enter__ = MagicMock(return_value=mock_session)
            MockSession.return_value.__exit__ = MagicMock(return_value=False)

            store.call(mock_req)

            # Verify prepare_auth was called with basic auth credentials
            mock_req.prepare_auth.assert_called_once_with(("user", "pass"))

    def test_basic_auth_env_var_fallback(self) -> None:
        with patch.dict(os.environ, {"RESONATE_USERNAME": "envuser", "RESONATE_PASSWORD": "envpass"}, clear=True):
            store = RemoteStore()
            assert store._auth == ("envuser", "envpass")
            assert store._token is None


class TestPollerTokenAuth:
    def test_token_param_sets_token(self) -> None:
        poller = Poller(group="default", id="test", token="test-token-123")
        assert poller._token == "test-token-123"

    def test_token_env_var_fallback(self) -> None:
        with patch.dict(os.environ, {"RESONATE_TOKEN": "env-token-456"}):
            poller = Poller(group="default", id="test")
            assert poller._token == "env-token-456"

    def test_token_param_takes_priority_over_env_var(self) -> None:
        with patch.dict(os.environ, {"RESONATE_TOKEN": "env-token"}):
            poller = Poller(group="default", id="test", token="arg-token")
            assert poller._token == "arg-token"

    def test_token_takes_priority_over_basic_auth(self) -> None:
        poller = Poller(group="default", id="test", token="my-token", auth=("user", "pass"))
        assert poller._token == "my-token"
        assert poller._auth == ("user", "pass")

    def test_no_token_when_not_provided(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            poller = Poller(group="default", id="test")
            assert poller._token is None

    def test_bearer_header_used_in_polling(self) -> None:
        poller = Poller(group="default", id="test", token="test-token-123", timeout=1)

        with patch("resonate.message_sources.poller.requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.raise_for_status = MagicMock()
            mock_response.iter_lines.return_value = iter([])
            mock_response.__enter__ = MagicMock(return_value=mock_response)
            mock_response.__exit__ = MagicMock(return_value=False)

            def stop_after_call(*args, **kwargs):
                poller._stopped = True
                return mock_response

            mock_get.side_effect = stop_after_call

            poller.loop()

            # Verify that requests.get was called with headers containing Bearer token
            mock_get.assert_called_once()
            call_kwargs = mock_get.call_args
            assert "headers" in call_kwargs.kwargs
            assert call_kwargs.kwargs["headers"]["Authorization"] == "Bearer test-token-123"
            # Verify auth was NOT passed (token takes priority)
            assert "auth" not in call_kwargs.kwargs

    def test_basic_auth_used_when_no_token(self) -> None:
        poller = Poller(group="default", id="test", auth=("user", "pass"), timeout=1)

        with patch("resonate.message_sources.poller.requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.raise_for_status = MagicMock()
            mock_response.iter_lines.return_value = iter([])
            mock_response.__enter__ = MagicMock(return_value=mock_response)
            mock_response.__exit__ = MagicMock(return_value=False)

            def stop_after_call(*args, **kwargs):
                poller._stopped = True
                return mock_response

            mock_get.side_effect = stop_after_call

            poller.loop()

            # Verify that requests.get was called with auth tuple
            mock_get.assert_called_once()
            call_kwargs = mock_get.call_args
            assert call_kwargs.kwargs["auth"] == ("user", "pass")
            # Verify headers was NOT passed (no token)
            assert "headers" not in call_kwargs.kwargs


class TestResonateRemoteTokenAuth:
    def test_remote_accepts_token_param(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            from resonate.resonate import Resonate

            r = Resonate.remote(token="test-token", host="http://localhost", store_port="9999", message_source_port="9998")
            assert r._store._token == "test-token"
            assert r._message_source._token == "test-token"

    def test_remote_token_env_var(self) -> None:
        with patch.dict(os.environ, {"RESONATE_TOKEN": "env-token"}, clear=True):
            from resonate.resonate import Resonate

            r = Resonate.remote(host="http://localhost", store_port="9999", message_source_port="9998")
            assert r._store._token == "env-token"
            assert r._message_source._token == "env-token"

    def test_remote_token_priority_over_auth(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            from resonate.resonate import Resonate

            r = Resonate.remote(token="my-token", auth=("user", "pass"), host="http://localhost", store_port="9999", message_source_port="9998")
            assert r._store._token == "my-token"
            assert r._message_source._token == "my-token"
            assert r._store._auth == ("user", "pass")
            assert r._message_source._auth == ("user", "pass")
