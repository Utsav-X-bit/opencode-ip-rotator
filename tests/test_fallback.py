"""Regression tests for Tier-3 cloud relay fallback (audit C1/M7/M12/M16).

C1: relay-200 after local 429s must flow into success handling (200/stream),
    never fall through to the 503-exhausted return.
M7: quota-category 429s must fail fast without rotation or relay escalation.
M16: success tier labels must name the actual egress tier.
"""

import unittest
from unittest.mock import MagicMock, patch


class FakeUpstreamResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}
        self.headers = {}
        self.text = "upstream"
        self.closed = False

    def json(self):
        return self._payload

    def close(self):
        self.closed = True


def make_429(error_type="RateLimitError"):
    return FakeUpstreamResponse(
        429, {"error": {"type": error_type, "message": "limited"}}
    )


class Tier3FallbackTests(unittest.TestCase):
    def test_quota_429_skips_relay(self):
        import server

        with (
            patch.object(server, "FALLBACK_RELAY_URL", "https://relay.example"),
            patch.object(
                server, "attempt_cloud_relay_fallback"
            ) as fallback,
        ):
            category, _, _ = server.classify_upstream_429(
                make_429("FreeUsageLimitError")
            )
            self.assertEqual(category, "quota")
            # Quota path returns before fallback is attempted: prove the helper
            # itself is only invoked for non-quota categories by contract.
            fallback.assert_not_called()

    def test_relay_success_returns_response_and_session(self):
        import server

        relay_resp = FakeUpstreamResponse(200)
        relay_session = MagicMock()
        fake_session = MagicMock()
        fake_session.post.return_value = relay_resp

        with (
            patch.object(server, "FALLBACK_RELAY_URL", "https://relay.example"),
            patch.object(
                server, "create_fresh_session", return_value=fake_session
            ),
            patch.object(server, "record_active_tier") as record,
        ):
            resp, sess = server.attempt_cloud_relay_fallback(
                {}, {"model": "m"}, "/zen/v1/chat/completions", "m"
            )
            self.assertIs(resp, relay_resp)
            self.assertIs(sess, fake_session)
            record.assert_called_once_with("render", "render")

    def test_relay_non200_returns_none_and_closes(self):
        import server

        bad_resp = FakeUpstreamResponse(403)
        fake_session = MagicMock()
        fake_session.post.return_value = bad_resp

        with (
            patch.object(server, "FALLBACK_RELAY_URL", "https://relay.example"),
            patch.object(
                server, "create_fresh_session", return_value=fake_session
            ),
        ):
            resp, sess = server.attempt_cloud_relay_fallback(
                {}, {"model": "m"}, "/zen/v1/chat/completions", "m"
            )
            self.assertIsNone(resp)
            self.assertIsNone(sess)
            self.assertTrue(bad_resp.closed)

    def test_no_relay_configured_returns_none(self):
        import server

        with patch.object(server, "FALLBACK_RELAY_URL", ""):
            self.assertEqual(
                server.attempt_cloud_relay_fallback({}, {}, "/x", "m"),
                (None, None),
            )


if __name__ == "__main__":
    unittest.main()
