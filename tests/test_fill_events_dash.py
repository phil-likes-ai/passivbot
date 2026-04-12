import pytest

from tools.fill_events_dash import _resolve_bind_host, parse_args


def test_fill_events_dash_parse_args_defaults_to_localhost():
    args = parse_args(["--users", "acct01"])

    assert args.host == "127.0.0.1"
    assert args.allow_insecure_bind is False


def test_fill_events_dash_rejects_public_bind_without_explicit_flag():
    with pytest.raises(
        ValueError, match="refusing non-local fill-events dashboard bind host 0.0.0.0"
    ):
        _resolve_bind_host("0.0.0.0", allow_insecure_bind=False)


def test_fill_events_dash_allows_public_bind_with_explicit_flag():
    assert _resolve_bind_host("0.0.0.0", allow_insecure_bind=True) == "0.0.0.0"
