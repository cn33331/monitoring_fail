import sys
import os
from PyQt6.QtWidgets import (QApplication, QDialog, QStyleFactory)
from PyQt6.QtCore import QDateTime, Qt
from ui.FilterConfigInfo import Ui_Form  # 从生成的UI文件导入

if getattr(sys, 'frozen', False):
    # 已打包状态：获取可执行文件所在路径
    app_path = os.path.dirname(sys.executable)
else:
    # 未打包状态：获取当前Python文件所在路径
    app_path = os.path.dirname(os.path.abspath(__file__))

file_path = os.path.join(app_path, "test_data.db")

class FilterConfigInfoUI(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setParent(parent)
        # 方式1：加载生成的UI代码
        self.ui = Ui_Form()
        self.ui.setupUi(self)
        QApplication.setStyle(QStyleFactory.create("Fusion"))#确认ui的样式
        self.setWindowTitle("筛选配置")
        self.setModal(True)  # 模态窗口（阻塞主窗口操作）

    def get_start_datetime(self):
        """获取开始时间"""
        start_time_str = self.ui.dateTimeEdit_start.dateTime()
        start_time_str = start_time_str.toString(Qt.DateFormat.ISODateWithMs)
        start_time_str = start_time_str.replace('T', ' ')[:19]
        return start_time_str

    def get_end_datetime(self):
        """获取结束时间"""
        end_time_str = self.ui.dateTimeEdit_end.dateTime()
        end_time_str = end_time_str.toString(Qt.DateFormat.ISODateWithMs)
        end_time_str = end_time_str.replace('T', ' ')[:19]
        return end_time_str

    def get_slot_id_exclude_str(self):
        """获取slot_id排除字符串"""
        return "pass"

    def get_test_item_exclude_str(self):
        """获取test_item排除字符串"""
        return self.ui.textEdit_shielding_test_name.toPlainText().strip()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = FilterConfigInfoUI()
    window.exec()  # QDialog用exec()显示（模态），而非show()
    sys.exit(app.exec())