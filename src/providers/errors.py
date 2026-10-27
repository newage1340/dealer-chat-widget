"""
Errors every provider raises, so callers can react without knowing the vendor.
"""


class ProviderError(Exception):
    """
    A provider call failed.

    `retryable` tells a router whether trying again, or falling back to
    another provider, could plausibly succeed.
    """

    retryable: bool = False

    def __init__(self, provider: str, message: str, *, retryable: bool | None = None):
        super().__init__(f"{provider}: {message}")
        self.provider = provider
        if retryable is not None:
            self.retryable = retryable


class ProviderTimeout(ProviderError):
    retryable = True


class ProviderUnavailable(ProviderError):
    """
    The provider is down, overloaded or rate-limiting us.
    """

    retryable = True
