# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['/Users/gdlocal/Desktop/myCode/monitoring_fail/only_fail.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'matplotlib', 'scipy',  # 排除未使用的大型库
        'tkinter', 'unittest', 'pydoc',  # 排除不必要的标准库
        'setuptools', 'pip', 'distutils'
    ],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='only_fail',
    debug=True,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
app = BUNDLE(
    exe,
    name='监控fail.app',
    icon='/Users/gdlocal/Desktop/myCode/monitoring_fail/static/1.ico',
    bundle_identifier=None,
    version='0.1'  # 添加版本号
)
