import logging

from ..config import cfg
from .base import BaseGateway
from .cli import CliGateway
from .telegram import TelegramGateway

logger = logging.getLogger(__name__)

_REGISTRY: dict[str, type[BaseGateway]] = {
    "cli": CliGateway,
    "telegram": TelegramGateway,
}


def load_gateways(*, hub_url: str) -> list[BaseGateway]:
    active = cfg.gateway.get("active", sorted(_REGISTRY.keys()))

    gateways: list[BaseGateway] = []
    for name in active:
        cls = _REGISTRY.get(name)
        if cls is None:
            logger.warning("Unknown gateway type: %s", name)
            continue

        gateway_cfg = cfg.gateway.get(name, {})
        try:
            gateways.append(cls.from_config(hub_url=hub_url, gateway_cfg=gateway_cfg))
        except ValueError as exc:
            logger.debug("Skip gateway %s: %s", name, exc)

    return gateways


def available_gateway_types() -> list[str]:
    return sorted(_REGISTRY.keys())
