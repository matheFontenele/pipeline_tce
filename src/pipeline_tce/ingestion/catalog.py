"""Carrega e valida os catálogos versionados da API do TCE-CE."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit


SUPPORTED_FREQUENCIES = {"mensal", "anual"}
SUPPORTED_API_PARAMETERS = {
    "exercicio_orcamento",
    "codigo_municipio",
    "data_referencia_doc",
    "data_inicio",
    "data_fim",
}


@dataclass(frozen=True)
class Endpoint:
    display_name: str
    url: str
    prefix: str
    frequency: str
    required_parameters: tuple[str, ...]


@dataclass(frozen=True)
class Municipality:
    code: str
    name: str


class CatalogError(ValueError):
    """Indica catálogo ausente ou inconsistente."""


def catalog_directory() -> Path:
    configured = os.getenv("TCE_CATALOG_DIR")
    if configured:
        return Path(configured)
    return Path(__file__).resolve().parents[3] / "catalog"


def load_endpoints(path: Path | None = None) -> dict[str, Endpoint]:
    source = path or catalog_directory() / "endpoints.json"
    try:
        raw: dict[str, Any] = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CatalogError(f"Não foi possível carregar o catálogo de endpoints: {source}") from exc
    if not isinstance(raw, dict):
        raise CatalogError("O catálogo de endpoints deve ser um objeto JSON")

    endpoints: dict[str, Endpoint] = {}
    for name, item in raw.items():
        try:
            endpoint = Endpoint(
                display_name=name,
                url=item["endpoint"],
                prefix=item["prefixo"],
                frequency=item["frequencia"].strip().lower(),
                required_parameters=tuple(item["parametros_obrigatorios"]),
            )
        except (KeyError, AttributeError, TypeError) as exc:
            raise CatalogError(f"Definição inválida para endpoint {name!r}") from exc

        if not isinstance(endpoint.url, str) or not isinstance(endpoint.prefix, str):
            raise CatalogError(f"URL/prefixo inválido em {name!r}")
        if not all(isinstance(param, str) for param in endpoint.required_parameters):
            raise CatalogError(f"Lista de parâmetros inválida em {name!r}")
        parsed_url = urlsplit(endpoint.url)
        if (
            parsed_url.scheme != "https"
            or parsed_url.hostname != "api-dados-abertos.tce.ce.gov.br"
            or parsed_url.username is not None
            or parsed_url.password is not None
        ):
            raise CatalogError(f"Host não permitido no endpoint {name!r}")
        if endpoint.frequency not in SUPPORTED_FREQUENCIES:
            raise CatalogError(f"Frequência não suportada em {name!r}: {endpoint.frequency}")
        unknown = set(endpoint.required_parameters) - SUPPORTED_API_PARAMETERS
        if unknown:
            raise CatalogError(f"Parâmetros sem implementação em {name!r}: {sorted(unknown)}")
        if not endpoint.prefix or endpoint.prefix in {entry.prefix for entry in endpoints.values()}:
            raise CatalogError(f"Prefixo ausente ou duplicado em {name!r}")
        if not endpoint.required_parameters or "codigo_municipio" not in endpoint.required_parameters:
            raise CatalogError(f"Endpoint sem parâmetros de partição suportados: {name!r}")

        endpoints[endpoint.prefix] = endpoint

    if not endpoints:
        raise CatalogError("O catálogo de endpoints está vazio")
    return endpoints


def load_municipalities(path: Path | None = None) -> dict[str, Municipality]:
    source = path or catalog_directory() / "municipios.json"
    try:
        raw = json.loads(source.read_text(encoding="utf-8"))
        elements = raw["elements"]
    except (OSError, json.JSONDecodeError, KeyError, TypeError) as exc:
        raise CatalogError(f"Não foi possível carregar o catálogo de municípios: {source}") from exc
    if not isinstance(elements, list):
        raise CatalogError("A lista de municípios deve ser um array JSON")

    municipalities: dict[str, Municipality] = {}
    for item in elements:
        try:
            raw_code = item["codigo_municipio"]
            raw_name = item["nome_municipio"]
        except (KeyError, TypeError) as exc:
            raise CatalogError("Definição inválida de município") from exc
        code = str(raw_code)
        name = str(raw_name) if isinstance(raw_name, str) else ""
        if not code.isdigit() or len(code) != 3 or not name:
            raise CatalogError(f"Código/nome inválido para município {code!r}")
        if code in municipalities:
            raise CatalogError(f"Código de município duplicado: {code}")
        municipalities[code] = Municipality(code=code, name=name)

    if not municipalities:
        raise CatalogError("O catálogo de municípios está vazio")
    return municipalities
