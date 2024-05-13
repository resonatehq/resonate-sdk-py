"""Traits implements by system classes."""

from __future__ import annotations

from abc import ABC, abstractmethod

from typing_extensions import Self


class Default(ABC):
    """Default trait."""

    @classmethod
    @abstractmethod
    def default(cls) -> Self:
        """Define default implementation of the class."""
        raise NotImplementedError
