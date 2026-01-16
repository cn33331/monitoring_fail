# -*- mode: python ; coding: utf-8 -*-

import os
import sys

# 获取 spec 文件所在的目录（修正版本）
def get_base_path():
    """获取项目根目录路径"""
    # 方法1：使用 sys.argv[0] 获取当前文件路径
    if hasattr(sys, '_MEIPASS'):
        # 如果是打包后的运行时
        return sys._MEIPASS
    else:
        # 尝试获取 spec 文件所在目录
        spec_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
        return spec_dir

base_path = get_base_path()

# 打印调试信息
print(f"Base path: {base_path}")

added_files = [
    (os.path.join(base_path, 'config'), 'MacOS/config'),
    (os.path.join(base_path, 'sop.md'), 'MacOS'),
]

a = Analysis(
    ['only_fail.py'],  # 使用相对路径
    pathex=[base_path],  # 添加项目路径到搜索路径
    binaries=[],
    datas=added_files,
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'matplotlib', 'scipy',
        'tkinter', 'unittest', 'pydoc',
        'setuptools', 'pip', 'distutils',
        'test', 'tests', '*.test', '*.tests'
    ],
    noarchive=False,
    optimize=1,  # 优化级别设为1
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='only_fail',
    debug=False,  # 发布版本设为False
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,  # 设置为 True 如果需要在 macOS 终端显示输出
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
app = BUNDLE(
    exe,
    name='监控fail.app',
    icon=os.path.join(base_path, 'static', '1.ico'),  # 使用正确的路径
    bundle_identifier='com.yourcompany.monitoringfail',
    version='0.2',
    info_plist={
        'CFBundleName': '监控fail',
        'CFBundleDisplayName': '监控fail',
        'CFBundleVersion': '0.2',
        'CFBundleShortVersionString': '0.2',
        'NSHighResolutionCapable': 'True',
        'LSMinimumSystemVersion': '10.13',  # 支持 macOS High Sierra 及以上
    }
)