"""tests/test_host_resolver.py"""

import pytest
from port_manager.host_resolver import resolve_host, is_loopback, is_wildcard


class TestResolveHost:
    def test_none_returns_default(self):
        assert resolve_host(None) == "127.0.0.1"

    def test_localhost_resolves_to_ipv4(self):
        assert resolve_host("localhost") == "127.0.0.1"

    def test_ipv6_loopback_resolves_to_ipv4(self):
        assert resolve_host("::1") == "127.0.0.1"

    def test_wildcard_preserved(self):
        assert resolve_host("0.0.0.0") == "0.0.0.0"

    def test_ipv4_preserved(self):
        assert resolve_host("127.0.0.1") == "127.0.0.1"

    def test_custom_ip_preserved(self):
        assert resolve_host("192.168.1.1") == "192.168.1.1"

    def test_whitespace_stripped(self):
        assert resolve_host("  localhost  ") == "127.0.0.1"


class TestIsLoopback:
    def test_ipv4_loopback(self):
        assert is_loopback("127.0.0.1") is True

    def test_localhost(self):
        assert is_loopback("localhost") is True

    def test_wildcard_not_loopback(self):
        assert is_loopback("0.0.0.0") is False


class TestIsWildcard:
    def test_wildcard_ipv4(self):
        assert is_wildcard("0.0.0.0") is True

    def test_localhost_not_wildcard(self):
        assert is_wildcard("localhost") is False
