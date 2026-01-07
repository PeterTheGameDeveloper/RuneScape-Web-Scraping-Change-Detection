"""Test configuration and fixtures."""
import pytest


def pytest_configure(config):
    """Configure custom markers."""
    config.addinivalue_line(
        "markers", "integration: marks tests that hit external APIs (deselect with '-m \"not integration\"')"
    )
