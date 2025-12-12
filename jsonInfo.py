import sys
import json
import os
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QLineEdit, QCheckBox, QSpinBox, QPushButton, QLabel)
from PyQt6.QtCore import Qt, QObject, pyqtSlot

class JsonComponentBinder(QObject):
    """JSON与PyQt6组件双向绑定管理器"""
    def __init__(self, json_path: str):
        super().__init__()
        self.json_path = json_path  # JSON文件路径
        self.component_map = {}     # 组件映射表：{配置键: (组件, 属性名, 信号)}
        self.default_config = {}    # 默认配置（组件无值时使用）
        
        # 初始化：加载JSON文件（不存在则创建）
        self.config = self._load_json()

    def _load_json(self) -> dict:
        """加载JSON配置文件（不存在则返回空字典）"""
        if not os.path.exists(self.json_path):
            with open(self.json_path, 'w', encoding='utf-8') as f:
                json.dump({}, f, ensure_ascii=False, indent=4)
            return {}
        try:
            with open(self.json_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError:
            print(f"JSON文件格式错误：{self.json_path}，使用空配置")
            return {}

    def _save_json(self):
        """保存配置到JSON文件（线程安全）"""
        try:
            with open(self.json_path, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, ensure_ascii=False, indent=4)
        except Exception as e:
            print(f"保存JSON失败：{e}")

    def bind_component(self, config_key: str, component, prop_name: str, signal=None):
        """
        绑定组件与JSON配置键
        :param config_key: JSON中的键名（如"username"）
        :param component: PyQt6组件实例（如QLineEdit）
        :param prop_name: 组件属性名（如"text"、"checked"、"value"）
        :param signal: 组件属性变化的信号（如textChanged、stateChanged）
        """
        # 1. 记录组件映射关系
        self.component_map[config_key] = (component, prop_name, signal)
        
        # 2. 加载JSON配置到组件
        default_val = self.default_config.get(config_key, "")
        val = self.config.get(config_key, default_val)
        self._set_component_prop(component, prop_name, val)
        
        # 3. 绑定信号：组件变化时同步到JSON
        if signal:
            # 根据信号类型适配槽函数
            if prop_name in ["text", "checked", "value"]:
                signal.connect(lambda v: self._on_component_change(config_key, v))
            else:
                signal.connect(lambda: self._on_component_change(config_key, self._get_component_prop(component, prop_name)))

    def set_default_config(self, default_config: dict):
        """设置默认配置（组件无值时使用）"""
        self.default_config = default_config

    def _get_component_prop(self, component, prop_name: str):
        """获取组件属性值"""
        if hasattr(component, f"get{prop_name.capitalize()}"):
            # 适配getText()/getValue()等方法
            return getattr(component, f"get{prop_name.capitalize()}")()
        return getattr(component, prop_name)

    def _set_component_prop(self, component, prop_name: str, value):
        """设置组件属性值"""
        if hasattr(component, f"set{prop_name.capitalize()}"):
            # 适配setText()/setChecked()等方法
            getattr(component, f"set{prop_name.capitalize()}")(value)
        else:
            setattr(component, prop_name, value)

    @pyqtSlot()
    def _on_component_change(self, config_key: str, value):
        """组件变化时同步到JSON"""
        self.config[config_key] = value
        self._save_json()
        # print(f"同步配置到JSON：{config_key} = {value}")  # 调试用，可删除

class ConfigWindow(QMainWindow):
    def __init__(self, json_binder: JsonComponentBinder):
        super().__init__()
        self.json_binder = json_binder
        self._init_ui()
        self._bind_components()

    def _init_ui(self):
        self.setWindowTitle("PyQt6组件与JSON双向绑定")
        self.setGeometry(100, 100, 400, 300)
        
        # 中心组件+布局
        central_widget = QWidget()
        layout = QVBoxLayout(central_widget)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(15)
        self.setCentralWidget(central_widget)
        
        # 创建测试组件
        self.username_edit = QLineEdit()
        self.username_edit.setPlaceholderText("请输入用户名")
        
        self.remember_check = QCheckBox("记住密码")
        
        self.age_spin = QSpinBox()
        self.age_spin.setRange(0, 120)
        self.age_spin.setPrefix("年龄：")
        
        self.save_btn = QPushButton("手动保存配置（可选）")
        self.save_btn.clicked.connect(self.json_binder._save_json)
        
        # 添加组件到布局
        layout.addWidget(QLabel("用户配置面板"))
        layout.addWidget(self.username_edit)
        layout.addWidget(self.remember_check)
        layout.addWidget(self.age_spin)
        layout.addWidget(self.save_btn)

    def _bind_components(self):
        """绑定组件与JSON"""
        # 1. 绑定QLineEdit（文本变化同步）
        self.json_binder.bind_component(
            config_key="username",
            component=self.username_edit,
            prop_name="text",
            signal=self.username_edit.textChanged
        )
        
        # 2. 绑定QCheckBox（勾选状态同步）
        self.json_binder.bind_component(
            config_key="remember_password",
            component=self.remember_check,
            prop_name="checked",
            signal=self.remember_check.stateChanged
        )
        
        # 3. 绑定QSpinBox（数值变化同步）
        self.json_binder.bind_component(
            config_key="age",
            component=self.age_spin,
            prop_name="value",
            signal=self.age_spin.valueChanged
        )

if __name__ == "__main__":
    # 1. 初始化应用
    app = QApplication(sys.argv)
    app.setAttribute(Qt.ApplicationAttribute.AA_DontUseNativeMenuBar, True)
    
    # 2. 初始化JSON绑定器（指定JSON文件路径）
    json_path = "/Users/gdlocal/Desktop/config.json"  # 替换为你的路径
    json_binder = JsonComponentBinder(json_path)
    
    # 3. 设置默认配置（首次启动时使用）
    json_binder.set_default_config({
        "username": "admin",
        "remember_password": True,
        "age": 25
    })
    
    # 4. 创建窗口并绑定组件
    window = ConfigWindow(json_binder)
    window.show()
    
    # 5. 运行应用
    sys.exit(app.exec())