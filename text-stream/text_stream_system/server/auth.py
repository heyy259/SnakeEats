"""Token-based authentication for TextStreamSystem."""


class AuthManager:
    """Simple token-based authentication.

    If *auth_token* is empty, all connections are accepted (no-auth mode).
    Otherwise, the first message must be ``{"method":"auth","params":{"token":"..."}}``
    before any other RPC call is processed.
    """

    def __init__(self, auth_token: str = ""):
        self._auth_token = auth_token

    @property
    def requires_auth(self) -> bool:
        return bool(self._auth_token)

    def authenticate(self, token: str) -> bool:
        """Return True if *token* matches the configured auth token.

        When auth_token is empty, always returns True.
        """
        if not self._auth_token:
            return True
        return token == self._auth_token
