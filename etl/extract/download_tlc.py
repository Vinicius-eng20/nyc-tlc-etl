# =============================================================================
# NYC TLC ETL — Extract
# Responsabilidade: baixar o arquivo Parquet mensal do NYC TLC
# =============================================================================

import os
import logging
import requests
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)


TLC_BASE_URL = os.getenv(
    "TLC_BASE_URL",
    "https://d37ci6vzurychx.cloudfront.net/trip-data"
)
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", "/opt/airflow/data/raw")


def build_url(year: int, month: int) -> str:
    """Monta a URL do arquivo Parquet do TLC para o mês/ano informado."""
    month_str = str(month).zfill(2)
    return f"{TLC_BASE_URL}/yellow_tripdata_{year}-{month_str}.parquet"


def build_local_path(year: int, month: int) -> Path:
    """Retorna o caminho local onde o arquivo será salvo."""
    month_str = str(month).zfill(2)
    raw_dir = Path(RAW_DATA_PATH)
    raw_dir.mkdir(parents=True, exist_ok=True)
    return raw_dir / f"yellow_tripdata_{year}-{month_str}.parquet"


def download_parquet(year: int, month: int) -> str:
    """
    Baixa o arquivo Parquet mensal do NYC TLC.

    Args:
        year:  Ano de referência (ex: 2024)
        month: Mês de referência (ex: 1)

    Returns:
        Caminho local do arquivo baixado.

    Raises:
        ValueError: Se ano ou mês forem inválidos.
        requests.HTTPError: Se o download falhar.
        IOError: Se não for possível salvar o arquivo.
    """
    _validate_params(year, month)

    url = build_url(year, month)
    local_path = build_local_path(year, month)

    # Evita baixar novamente se o arquivo já existe e tem tamanho > 0
    if local_path.exists() and local_path.stat().st_size > 0:
        logger.info(
            "Arquivo já existe localmente, pulando download: %s", local_path
        )
        return str(local_path)

    logger.info("Iniciando download: %s", url)
    logger.info("Destino: %s", local_path)

    try:
        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logger.error("Falha no download (HTTP %s): %s", response.status_code, url)
        raise

    total_bytes = 0
    try:
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):  # 8MB
                if chunk:
                    f.write(chunk)
                    total_bytes += len(chunk)
    except IOError as e:
        # Remove arquivo parcial em caso de erro
        if local_path.exists():
            local_path.unlink()
        logger.error("Erro ao salvar arquivo: %s", e)
        raise

    size_mb = total_bytes / (1024 * 1024)
    logger.info(
        "Download concluído: %.2f MB salvos em %s", size_mb, local_path
    )

    return str(local_path)


def validate_file(year: int, month: int) -> dict:
    """
    Valida se o arquivo Parquet baixado existe e tem tamanho mínimo esperado.

    Args:
        year:  Ano de referência
        month: Mês de referência

    Returns:
        Dicionário com informações do arquivo: path, size_mb, is_valid.
    """
    local_path = build_local_path(year, month)

    if not local_path.exists():
        logger.error("Arquivo não encontrado: %s", local_path)
        return {"path": str(local_path), "size_mb": 0, "is_valid": False}

    size_bytes = local_path.stat().st_size
    size_mb = size_bytes / (1024 * 1024)

    # Arquivos do TLC têm pelo menos ~10MB para meses com dados reais
    is_valid = size_bytes > 10 * 1024 * 1024

    if not is_valid:
        logger.warning(
            "Arquivo suspeito (muito pequeno): %.2f MB — %s", size_mb, local_path
        )
    else:
        logger.info("Arquivo validado: %.2f MB — %s", size_mb, local_path)

    return {
        "path": str(local_path),
        "size_mb": round(size_mb, 2),
        "is_valid": is_valid,
    }


def _validate_params(year: int, month: int) -> None:
    """Valida os parâmetros de ano e mês."""
    current_year = datetime.now().year
    if not (2019 <= year <= current_year):
        raise ValueError(
            f"Ano inválido: {year}. Esperado entre 2019 e {current_year}."
        )
    if not (1 <= month <= 12):
        raise ValueError(f"Mês inválido: {month}. Esperado entre 1 e 12.")