import sys
import os
import warnings
# 屏蔽sip弃用警告（可选）
warnings.filterwarnings("ignore", category=DeprecationWarning)

# 必须导入的依赖
import mistune
from PyQt6.QtWidgets import (QApplication, QMainWindow, QTextBrowser, 
                             QVBoxLayout, QWidget)
from PyQt6.QtCore import Qt, QUrl
from PyQt6.QtGui import QDesktopServices

class MDViewer(QMainWindow):
    def __init__(self,md_path,browser):
        super().__init__()
        self.md_path = md_path
        self.md_browser = browser
        self._init_ui()
        self.load_md_file(self.md_path)
        
    def _init_ui(self):
        """初始化UI"""
        # 核心组件：改用QTextBrowser（支持anchorClicked信号）
        self.md_browser.setReadOnly(True)  # 只读
        self.md_browser.setStyleSheet("""
            QTextBrowser {
                font-size: 14px;
                padding: 10px;
                line-height: 1.6;
            }
        """)
        
        # 绑定链接点击信号（QTextBrowser专属）
        self.md_browser.anchorClicked.connect(self.open_link)

    def load_md_file(self, md_path):
        """加载并渲染Markdown文件（稳定版）"""
        # 1. 检查文件是否存在
        if not os.path.exists(md_path):
            self.md_browser.setText(f"<font color='red'>文件不存在：</font>{md_path}")
            return
        
        # 2. 读取MD文本
        try:
            with open(md_path, 'r', encoding='utf-8') as f:
                md_text = f.read()
        except Exception as e:
            self.md_browser.setText(f"<font color='red'>读取失败：</font>{str(e)}")
            return
        
        # 3. MD转HTML（mistune更稳定，原生支持表格/图片）
        renderer = mistune.create_markdown(
            plugins=['table', 'strikethrough', 'url']  # 启用常用插件
        )
        html = renderer(md_text)
        
        # 4. 处理本地图片路径（关键：转为file协议绝对路径）
        md_dir = os.path.dirname(os.path.abspath(md_path))
        # 简易替换：适配相对路径图片（无需BeautifulSoup）
        html = html.replace('src="', f'src="file://{md_dir}/')
        # 修复可能的重复file://问题（如图片路径已带协议）
        html = html.replace('file://file://', 'file://')

        # 5. 加载HTML
        self.md_browser.setHtml(html)

    def open_link(self, url: QUrl):
        """点击链接用系统浏览器打开"""
        if url.scheme() in ['http', 'https']:
            QDesktopServices.openUrl(url)
        # 本地文件链接也可处理（可选）
        elif url.scheme() == 'file':
            os.startfile(url.toLocalFile())  # Windows/macOS通用

# ===== 调用MDViewer类的Demo入口 =====
if __name__ == "__main__":
    # 1. 创建QApplication实例（PyQt6必须）
    app = QApplication(sys.argv)
    
    # 2. macOS系统适配（可选，避免UI错位）
    app.setAttribute(Qt.ApplicationAttribute.AA_DontUseNativeMenuBar, True)
    
    # 3. 初始化QTextBrowser（作为参数传给MDViewer）
    md_browser = QTextBrowser()
    
    # 4. 配置你的MD文件路径（替换为实际路径！）
    # 示例：/Users/gdlocal/Desktop/test.md
    test_md_path = "/Users/gdlocal/Desktop/myCode/monitoring_fail/sop.md"
    
    # 5. 创建MDViewer实例（传入MD路径和QTextBrowser）
    # 注意：MDViewer继承QMainWindow，需设置主窗口基础属性
    window = MDViewer(md_path=test_md_path, browser=md_browser)
    
    # 6. 设置主窗口基础样式（可选）
    window.setWindowTitle("Markdown阅读器 Demo")
    window.setGeometry(100, 100, 1000, 800)  # 窗口位置和大小
    
    # 7. 将QTextBrowser添加到主窗口布局（关键！否则控件不显示）
    central_widget = QWidget()
    layout = QVBoxLayout(central_widget)
    layout.setContentsMargins(0, 0, 0, 0)
    layout.addWidget(md_browser)
    window.setCentralWidget(central_widget)
    
    # 8. 显示窗口并运行程序
    window.show()
    sys.exit(app.exec())