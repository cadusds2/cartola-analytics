"""Core catalog of Cartola FC API endpoints used across the project."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class Endpoint:
    """Represents a Cartola FC endpoint."""

    name: str
    url: str
    requires_round: bool = False

    def resolve(self, rodada: int | None = None) -> str:
        """Return a usable URL, formatting round-aware endpoints."""
        if self.requires_round:
            if rodada is None:
                msg = f"Endpoint '{self.name}' requires a rodada value"
                raise ValueError(msg)
            return self.url.format(rodada=rodada)
        return self.url


_ENDPOINTS: Sequence[Endpoint] = (
    Endpoint(name="mercado_status", url="https://api.cartola.globo.com/mercado/status"),
    Endpoint(name="atletas_mercado", url="https://api.cartola.globo.com/atletas/mercado"),
    Endpoint(name="atletas_pontuados", url="https://api.cartola.globo.com/atletas/pontuados"),
    Endpoint(name="pos_rodada_destaques", url="https://api.cartola.globo.com/pos-rodada/destaques"),
    Endpoint(name="clubes", url="https://api.cartola.globo.com/clubes"),
    Endpoint(name="posicoes", url="https://api.cartola.globo.com/posicoes"),
    Endpoint(name="patrocinadores", url="https://api.cartola.globo.com/patrocinadores"),
    Endpoint(name="partidas", url="https://api.cartola.globo.com/partidas"),
    Endpoint(name="videos", url="https://api.cartola.globo.com/videos"),
    Endpoint(name="mercado_destaques", url="https://api.cartola.globo.com/mercado/destaques"),
    Endpoint(name="mercado_destaques_reservas", url="https://api.cartola.globo.com/mercado/destaques/reservas"),
    Endpoint(name="rodadas", url="https://api.cartola.globo.com/rodadas"),
    Endpoint(
        name="partidas_por_rodada",
        url="https://api.cartola.globo.com/partidas/{rodada}",
        requires_round=True,
    ),
    Endpoint(name="rankings", url="https://api.cartola.globo.com/rankings"),
    Endpoint(name="ligas", url="https://api.cartola.globo.com/ligas"),
    Endpoint(name="esquemas", url="https://api.cartolafc.globo.com/esquemas"),
)


def list_endpoints() -> list[Endpoint]:
    """Return a copy of the registered endpoints."""
    return list(_ENDPOINTS)


def iter_endpoints() -> Iterable[Endpoint]:
    """Iterate through registered endpoints."""
    return iter(_ENDPOINTS)


