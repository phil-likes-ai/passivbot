from importlib import import_module
import types

import pytest


cli_utils = import_module("fill_events_cli_utils")


def test_instantiate_bot_raises_when_user_missing():
    with pytest.raises(ValueError, match="Config missing live.user"):
        cli_utils.instantiate_bot({}, load_user_info=lambda user: {}, exchange_bot_classes={})


def test_instantiate_bot_loads_registered_class(monkeypatch):
    module = types.SimpleNamespace(MyBot=lambda config: ("ok", config))
    monkeypatch.setattr(cli_utils, "import_module", lambda name: module)

    result = cli_utils.instantiate_bot(
        {"live": {"user": "alice"}},
        load_user_info=lambda user: {"exchange": "fake"},
        exchange_bot_classes={"fake": ("fake_module", "MyBot")},
    )

    assert result == ("ok", {"live": {"user": "alice"}})


@pytest.mark.asyncio
async def test_run_cli_invokes_manager_refresh_and_close(tmp_path):
    class FakeBot:
        def __init__(self):
            self.exchange = "fake"
            self.user = "alice"
            self.closed = False

        async def close(self):
            self.closed = True

    class FakeManager:
        def __init__(self, exchange, user, fetcher, cache_path):
            self.logger = types.SimpleNamespace(info=lambda *args, **kwargs: None)
            self.refresh_args = None
            self._events = [1, 2]

        async def refresh_range(self, start_ms, end_ms):
            self.refresh_args = (start_ms, end_ms)

        def get_events(self, start_ms, end_ms):
            return self._events

    bot = FakeBot()
    args = types.SimpleNamespace(config=None, user="alice", symbols=None, cache_root=str(tmp_path), start="1000", end="2000", lookback_days=30)

    await cli_utils.run_cli(
        args,
        load_input_config=lambda path: ({}, None, None),
        prepare_config=lambda source_config, **kwargs: {"live": {}},
        instantiate_bot_fn=lambda config: bot,
        extract_symbol_pool=lambda config, override: ["BTC"],
        build_fetcher_for_bot=lambda bot, symbols: object(),
        manager_cls=FakeManager,
        parse_time_arg_fn=lambda value: int(value) if value else None,
        format_ms_fn=lambda ts: str(ts),
        logger=types.SimpleNamespace(info=lambda *args, **kwargs: None),
    )

    assert bot.closed is True
