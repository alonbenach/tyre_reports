# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path


PROJECT_ROOT = Path.cwd()
ENTRYPOINT = PROJECT_ROOT / "database" / "tools" / "run_app_prod.py"
ASSETS_DIR = PROJECT_ROOT / "assets"
MIGRATIONS_DIR = PROJECT_ROOT / "database" / "migrations"

datas = []
if ASSETS_DIR.exists():
    datas.append((str(ASSETS_DIR), "assets"))
if MIGRATIONS_DIR.exists():
    datas.append((str(MIGRATIONS_DIR), "database/migrations"))

pathex = [str(PROJECT_ROOT), str(PROJECT_ROOT / "src")]

a = Analysis(
    [str(ENTRYPOINT)],
    pathex=pathex,
    binaries=[],
    datas=datas,
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="MotoWeeklyOperator",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="MotoWeeklyOperator",
)
