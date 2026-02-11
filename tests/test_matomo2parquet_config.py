from pathlib import Path

from config import resolve_chunk_size


def test_requirements_includes_psutil():
    requirements_path = Path(__file__).resolve().parents[1] / "requirements.txt"
    requirements = requirements_path.read_text(encoding="utf-8").splitlines()
    normalized = {line.strip().lower() for line in requirements if line.strip()}
    assert "psutil" in normalized


def test_chunk_size_empty_uses_default_10000():
    chunk_size, used_default = resolve_chunk_size("")
    assert chunk_size == 10000
    assert used_default is True


def test_chunk_size_nonnumeric_uses_default_10000():
    chunk_size, used_default = resolve_chunk_size("abc")
    assert chunk_size == 10000
    assert used_default is True


def test_chunk_size_zero_or_negative_uses_default_10000():
    chunk_size_zero, used_default_zero = resolve_chunk_size("0")
    chunk_size_negative, used_default_negative = resolve_chunk_size("-1")

    assert chunk_size_zero == 10000
    assert used_default_zero is True
    assert chunk_size_negative == 10000
    assert used_default_negative is True


def test_chunk_size_valid_value_is_used():
    chunk_size, used_default = resolve_chunk_size("25000")
    assert chunk_size == 25000
    assert used_default is False
