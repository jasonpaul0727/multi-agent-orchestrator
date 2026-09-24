"""Run-scoped Agent instance accounting and lifecycle."""

from .registry import (
    AgentConcurrencyLimitExceeded,
    AgentDepthLimitExceeded,
    AgentInstance,
    AgentLimitExceeded,
    AgentRegistry,
    AgentRegistryError,
    AgentRegistryLimits,
    AgentRegistryState,
    reduce_agent_registry,
)

__all__ = [
    "AgentConcurrencyLimitExceeded",
    "AgentDepthLimitExceeded",
    "AgentInstance",
    "AgentLimitExceeded",
    "AgentRegistry",
    "AgentRegistryError",
    "AgentRegistryLimits",
    "AgentRegistryState",
    "reduce_agent_registry",
]
