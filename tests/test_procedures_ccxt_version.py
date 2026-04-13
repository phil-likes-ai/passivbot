from __future__ import annotations

from types import SimpleNamespace

import procedures


def test_load_ccxt_version_prefers_package_metadata(monkeypatch):
    monkeypatch.setattr(
        procedures.importlib_metadata,
        "distribution",
        lambda _name: SimpleNamespace(requires=["numpy==1.26.4", "ccxt==4.5.22"]),
    )

    assert procedures.load_ccxt_version() == "4.5.22"


def test_load_ccxt_version_falls_back_to_requirements_file(tmp_path, monkeypatch):
    root = tmp_path
    src_dir = root / "src"
    src_dir.mkdir()
    requirements = root / "requirements-live.txt"
    requirements.write_text("ccxt==4.5.99\nnumpy==1.26.4\n", encoding="utf-8")

    monkeypatch.setattr(
        procedures.importlib_metadata,
        "distribution",
        lambda _name: (_ for _ in ()).throw(procedures.importlib_metadata.PackageNotFoundError),
    )
    monkeypatch.setattr(procedures, "__file__", str(src_dir / "procedures.py"))

    assert procedures.load_ccxt_version() == "4.5.99"


def test_load_user_info_accepts_utf8_bom(tmp_path):
    api_keys = tmp_path / "api-keys.json"
    api_keys.write_text(
        '\ufeff{"fake_hsl_runner":{"exchange":"fake","quote":"USDT"}}',
        encoding="utf-8",
    )

    result = procedures.load_user_info("fake_hsl_runner", api_keys_path=str(api_keys))

    assert result["exchange"] == "fake"
    assert result["quote"] == "USDT"
