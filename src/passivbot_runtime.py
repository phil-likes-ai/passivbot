from __future__ import annotations

import asyncio
import logging
import signal


def signal_handler(sig, frame):
    """Handle SIGINT by signalling the running bot to stop gracefully."""
    del sig, frame
    logging.info("Received shutdown signal. Stopping bot...")
    bot = globals().get("bot")
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = None

    if bot is not None:
        bot.stop_signal_received = True
        if loop is not None:
            shutdown_task = getattr(bot, "_shutdown_task", None)
            if shutdown_task is None or shutdown_task.done():
                bot._shutdown_task = loop.create_task(bot.shutdown_gracefully())
            loop.call_soon_threadsafe(lambda: None)
    elif loop is not None:
        loop.call_soon_threadsafe(loop.stop)


def register_signal_handlers():
    """Register process signal handlers explicitly at runtime."""
    signal.signal(signal.SIGINT, signal_handler)


async def shutdown_bot(bot):
    """Stop background tasks and close the exchange clients gracefully."""
    logging.info("Shutting down bot...")
    bot.stop_data_maintainers()
    try:
        await asyncio.wait_for(bot.close(), timeout=3.0)
    except asyncio.TimeoutError:
        logging.warning("Shutdown timed out after 3 seconds. Forcing exit.")
    except Exception as e:
        logging.exception("Error during shutdown: %s", e)


async def close_bot_clients(bot) -> None:
    """Best-effort close of CCXT clients with logged failures."""
    try:
        bot.stop_data_maintainers()
        if bot.ccp is not None:
            await bot.ccp.close()
        if bot.cca is not None:
            await bot.cca.close()
    except Exception:
        logging.exception("error while closing bot clients during restart loop")
