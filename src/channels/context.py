"""
What every channel blueprint needs from the application.
"""

from dataclasses import dataclass

from config import Config
from pipeline import ActionExecutor, Services, TurnRunner


@dataclass(frozen=True, slots=True, kw_only=True)
class ChannelContext:
    config:   Config
    services: Services
    runner:   TurnRunner
    executor: ActionExecutor
