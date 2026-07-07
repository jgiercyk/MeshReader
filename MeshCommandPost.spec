# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_submodules

hiddenimports = ['paho.mqtt.client', 'paho.mqtt']
hiddenimports += collect_submodules('paho')
hiddenimports += collect_submodules('Crypto')


a = Analysis(
    ['C:\\Users\\jim\\MQTTReader\\mesh-command-post\\src\\main.py'],
    pathex=['C:\\Users\\jim\\MQTTReader\\mesh-command-post\\src'],
    binaries=[],
    datas=[],
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='MeshCommandPost',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='MeshCommandPost',
)
