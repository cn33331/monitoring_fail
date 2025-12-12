import sys
import os,json
import sqlite3
import pandas as pd
import queue
from PyQt6.QtWidgets import (QApplication, QWidget, QVBoxLayout,QHeaderView,
                             QPushButton, QLineEdit, QLabel, QTableWidget, 
                             QTableWidgetItem,QMessageBox,QAbstractItemView)
from PyQt6.QtCore import QTimer, QDateTime, Qt, QUrl, QThread, pyqtSignal
from PyQt6.QtGui import QDesktopServices, QColor
from PyQt6.QtWidgets import QStyleFactory
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pathlib import Path

from ui.main import Ui_ui_test  # 从生成的UI文件导入
from monitoringCSV import BasicFileHandler
from dataSQL import TestData
from readMD import MDViewer
from jsonInfo import JsonComponentBinder


if getattr(sys, 'frozen', False):
    # 已打包状态：获取可执行文件所在路径
    app_path = os.path.dirname(sys.executable)
else:
    # 未打包状态：获取当前Python文件所在路径
    app_path = os.path.dirname(os.path.abspath(__file__))

file_path = os.path.join(app_path, "test_data.db")
CONFIG_PATH = os.path.join(app_path, 'config.json')
SOP_MD_PATH = os.path.join(app_path, 'sop.md')


def enable_drag_drop(line_edit: QLineEdit):
    """
    使给定的 QLineEdit 具有拖放功能。
    """
    def dragEnterEvent(event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
        else:
            event.ignore()

    def dragMoveEvent(event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
        else:
            event.ignore()

    def dropEvent(event):
        if event.mimeData().hasUrls():
            urls = event.mimeData().urls()
            if urls:
                # 获取第一个文件/文件夹的路径
                path = urls[0].toLocalFile()
                line_edit.setText(path)
            event.acceptProposedAction()
        else:
            event.ignore()

    # 设置允许拖放事件
    line_edit.setAcceptDrops(True)

    # 重写拖放事件处理方法
    line_edit.dragEnterEvent = dragEnterEvent
    line_edit.dragMoveEvent = dragMoveEvent
    line_edit.dropEvent = dropEvent

class MonitorThread(QThread):
    """监控线程，避免阻塞UI"""
    update_signal = pyqtSignal()  # 添加信号，用于触发UI更新
    delete_signal = pyqtSignal()

    def __init__(self, monitor_dir, test_data,):
        super().__init__()
        self.monitor_dir = monitor_dir
        self.test_data = test_data
        self.handler = BasicFileHandler(self.on_file_updated,self.on_dir_deleted_callback)  # 改用内部回调

    def on_file_updated(self):
        """线程内回调，通过信号通知主线程"""
        self.update_signal.emit()  # 发送信号到主线程

    def on_dir_deleted_callback(self):
        """线程内回调，通过信号通知主线程"""
        self.delete_signal.emit()  # 发送信号到主线程

    def run(self):
        self.observer = self.handler.start(self.monitor_dir, self.test_data)
        while not self.isInterruptionRequested():
            self.msleep(1000)  # 每秒检查一次中断请求

    def stop(self):
        if self.observer:
            self.observer.stop()
            self.observer.join()
        self.requestInterruption()
        self.wait()

class failInfoWindow(QWidget, Ui_ui_test):
    def __init__(self):
        super().__init__()
        self.setupUi(self)  # 初始化UI
        QApplication.setStyle(QStyleFactory.create("Fusion"))#确认ui的样式

        # 初始化sop_UI,加载md文件并显示
        self.init_sop_ui()
        # 初始化ui的配置信息
        self.init_json_info()

        #初始化一些参数
        self.db_path = file_path #数据库文件地址
        self.monitor_dir = Path("~/Library/Logs/Atlas/unit-archive").expanduser() #被监控的文件夹地址
        self.monitor_thread = None #监控线程
        self.test_data = None #数据库类的实例

        #初始化时间标签，显示监听事件～当前时间
        self.init_time_range_label()
        #初始化显示fail信息的表格
        self.init_table_fail()

        #获取fail-csv的文件夹路径
        enable_drag_drop(self.textEdit_logpath)

        #清除数据并重启监控，删掉数据库，还有表格的内容
        self.pushButton_clear.clicked.connect(self.clear_status)
        #获取指定文件夹里的fail数据，存在数据库中并显示在ui上
        self.pushButton_get_failcsv.clicked.connect(self.get_fail_csv)
        #修改通道id文本框的状态，不允许随意修改
        self.pushButton_slotid_name.clicked.connect(self._toggle_edit_state)

        #开启监控线程
        self.init_monitoring()
    
    def _toggle_edit_state(self):
        current_state = self.lineEdit_slotid_name.isReadOnly()
        new_state = not current_state
        self.lineEdit_slotid_name.setReadOnly(new_state)

    def init_sop_ui(self):
        MDViewer(md_path=SOP_MD_PATH, browser=self.textBrowser_md)

    def init_json_info(self):
        self.json_binder = JsonComponentBinder(CONFIG_PATH)
        self._bind_components()

    def _bind_components(self):
        """绑定组件与JSON"""
        # 1. 绑定QLineEdit（文本变化同步）
        self.json_binder.bind_component(
            config_key="slot_id_test_name",
            component=self.lineEdit_slotid_name,
            prop_name="text",
            signal=self.lineEdit_slotid_name.textChanged
        )

    def init_monitoring(self):
        """初始化监控系统"""
        # 确保监控目录存在
        self.monitor_dir.mkdir(parents=True,exist_ok=True)
        
        # 初始化数据库
        self.test_data = TestData(self.db_path)
        
        # 启动监控线程
        self.start_monitor_thread()
        # 更新UI
        self.update_table_fail()
        print("初始化监控系统")

    def start_monitor_thread(self):
        """启动监控线程"""
        if self.monitor_thread and self.monitor_thread.isRunning():
            self.monitor_thread.stop()
        
        self.monitor_thread = MonitorThread(
            self.monitor_dir, 
            self.test_data
        )
        # 关键：连接线程信号到UI更新方法（自动在主线程执行）
        self.monitor_thread.update_signal.connect(self.update_table_fail)
        self.monitor_thread.delete_signal.connect(self.init_monitoring)
        self.monitor_thread.start()

    def clear_status(self):
        """清除数据并重启监控"""
        try:
            # 停止当前监控
            if self.monitor_thread and self.monitor_thread.isRunning():
                self.monitor_thread.stop()

            # 删除数据库文件
            if os.path.exists(self.db_path):
                os.remove(self.db_path)
                print("🗑️ 数据库文件已删除")

            # 更新起始时间
            self.start_time = QDateTime.currentDateTime().toString(self.time_format)
            
            # 重新初始化数据库和监控
            self.test_data = TestData(self.db_path)
            self.start_monitor_thread()
            
            # 更新UI
            self.update_table_fail()
            QMessageBox.information(self, "成功", "已清除数据并重新开始监控")
            
        except Exception as e:
            QMessageBox.warning(self, "错误", f"操作失败: {str(e)}")
        

    def init_time_range_label(self):
        self.time_format = "yyyy-MM-dd HH:mm:ss"
        current_time = QDateTime.currentDateTime().toString(self.time_format)
        self.label_time.setText(f"{current_time}\n{current_time}")
        self.start_time = current_time

        self.init_timer()

    def init_timer(self):
        """初始化定时器（每秒刷新一次当前时间）"""
        self.update_timer = QTimer(self)
        self.update_timer.setInterval(1000)  # 1000ms = 1秒
        self.update_timer.timeout.connect(self.update_current_time)
        self.update_timer.start()  # 启动定时器

    def update_current_time(self):
        """实时更新时间显示"""
        current_time = QDateTime.currentDateTime().toString(self.time_format)
        self.label_time.setText(f"{self.start_time}\n{current_time}")

    def _get_table_sort_state(self):
        """获取当前表格的排序状态（排序列、升降序）"""
        header = self.tableWidget_fail.horizontalHeader()
        sort_column = header.sortIndicatorSection()  # 排序的列索引
        sort_order = header.sortIndicatorOrder()     # 升降序（Qt.AscendingOrder/Qt.DescendingOrder）
        return sort_column, sort_order

    def _restore_table_sort_state(self, sort_column, sort_order):
        """恢复表格的排序状态"""
        if sort_column != -1:  # -1表示未排序
            self.tableWidget_fail.sortByColumn(sort_column, sort_order)

    def init_table_fail(self):
        # 1. 设置列数（9列）
        self.tableWidget_fail.setColumnCount(10)
        # 2. 设置列标题
        self.tableWidget_fail.setHorizontalHeaderLabels([
            "SN", "通道号", "测试时间", "测试项", "上限", "测试值", "下限", "结果", "源文件路径", "操作"
        ])
        
        # 3. 列宽设置（支持手动拉动 + 初始自适应）
        header = self.tableWidget_fail.horizontalHeader()
        
        # 第一步：先让所有列自动适应内容
        self.tableWidget_fail.resizeColumnsToContents()
        
        # 第二步：设置各列的调整模式
        for col in range(9):
            header.setSectionResizeMode(col, QHeaderView.ResizeMode.Interactive)
            self.tableWidget_fail.setColumnWidth(col, 70)
        
        self.tableWidget_fail.setColumnWidth(0, 180)  # SN列
        self.tableWidget_fail.setColumnWidth(2, 150)  # 测试时间列
        self.tableWidget_fail.setColumnWidth(3, 180)  # 测试项列
        
        # 源文件路径列
        header.setSectionResizeMode(8, QHeaderView.ResizeMode.Interactive)
        current_width = self.tableWidget_fail.columnWidth(8)
        self.tableWidget_fail.setColumnWidth(8, max(current_width, 200))
        
        # 操作列
        header.setSectionResizeMode(9, QHeaderView.ResizeMode.Interactive)
        self.tableWidget_fail.setColumnWidth(9, 120)  # 按钮列宽
        
        # 初始表格设置
        self.tableWidget_fail.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.tableWidget_fail.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.tableWidget_fail.setSortingEnabled(True)  # 初始启用排序

    def update_table_fail(self):
        self.fail_data = self.test_data.get_fail_data()
        if self.fail_data.empty:
            self.tableWidget_fail.setRowCount(0)
            return

        # ========== 关键步骤1：暂停UI刷新 + 禁用排序 ==========
        self.tableWidget_fail.setUpdatesEnabled(False)
        self.tableWidget_fail.setSortingEnabled(False)  # 禁用排序，避免行号错乱
        
        # ========== 关键步骤2：记录当前排序状态 ==========
        sort_col, sort_order = self._get_table_sort_state()

        # ========== 步骤3：清空并填充表格 ==========
        self.tableWidget_fail.setRowCount(0)  # 清空表格
        total_rows = len(self.fail_data)
        self.tableWidget_fail.setRowCount(total_rows)

        # 填充数据（按原始数据索引，此时排序已禁用，行号和数据索引一致）
        for row_idx, (df_index, row) in enumerate(self.fail_data.iterrows()):
            # 封装创建红色Item的函数
            def create_red_item(text):
                item_text = str(text) if pd.notna(text) else ""
                item = QTableWidgetItem(item_text)
                item.setForeground(QColor(Qt.GlobalColor.red))
                # 关键：设置Item的用户数据，存储原始数据索引（用于按钮点击）
                item.setData(Qt.ItemDataRole.UserRole, df_index)
                return item

            # 0: SN
            self.tableWidget_fail.setItem(row_idx, 0, create_red_item(row['sn']))
            # 1: slot_id
            self.tableWidget_fail.setItem(row_idx, 1, create_red_item(row['slot_id']))
            # 2: 测试时间
            if pd.notna(row['test_time']):
                time_text = row['test_time'].strftime('%Y-%m-%d %H:%M:%S')
            else:
                time_text = ""
            self.tableWidget_fail.setItem(row_idx, 2, create_red_item(time_text))
            # 3: 测试项
            self.tableWidget_fail.setItem(row_idx, 3, create_red_item(row['test_item']))
            # 4: 上限
            self.tableWidget_fail.setItem(row_idx, 4, create_red_item(row['test_usl']))
            # 5: 测试值
            self.tableWidget_fail.setItem(row_idx, 5, create_red_item(row['test_value']))
            # 6: 下限
            self.tableWidget_fail.setItem(row_idx, 6, create_red_item(row['test_lsl']))
            # 7: 结果
            try:
                result_text = str(row['test_result']) if pd.notna(row['test_result']) else "无结果"
            except Exception as e:
                result_text = f"异常: {str(e)[:10]}"
            self.tableWidget_fail.setItem(row_idx, 7, create_red_item(result_text))
            # 8: 源文件路径
            self.tableWidget_fail.setItem(row_idx, 8, create_red_item(row['file_path']))

            # 9: 操作列 - 打开文件夹按钮（核心修复：不再依赖行号，改用Item的用户数据）
            open_button = QPushButton("打开文件夹", self.tableWidget_fail)
            # 绑定点击事件（通过闭包传递原始数据索引，而非行号）
            open_button.clicked.connect(
                lambda checked, idx=df_index: self.on_open_folder_clicked(idx)
            )
            self.tableWidget_fail.setCellWidget(row_idx, 9, open_button)

        # ========== 关键步骤4：恢复排序 + UI刷新 ==========
        self.tableWidget_fail.setUpdatesEnabled(True)
        # 先启用排序，再恢复之前的排序状态
        self.tableWidget_fail.setSortingEnabled(True)
        self._restore_table_sort_state(sort_col, sort_order)

        # 优化列宽（按钮列）
        self.tableWidget_fail.setColumnWidth(9, 120)

    def on_open_folder_clicked(self, df_index):
        """
        当“打开文件夹”按钮被点击时调用
        :param df_index: DataFrame 中该行数据的真实索引标签
        """
        try:
            # 使用 .loc 按索引标签安全地获取行数据
            row_data = self.fail_data.loc[df_index]
            file_path = row_data['file_path']
        except KeyError:
            QMessageBox.warning(self, "错误", f"无法在数据中找到索引为 {df_index} 的行。")
            return

        if not file_path or pd.isna(file_path):
            QMessageBox.warning(self, "警告", "文件路径为空或无效。")
            return

        folder_path = os.path.dirname(str(file_path)) # 确保 file_path 是字符串

        if not os.path.exists(folder_path):
            QMessageBox.warning(self, "警告", f"文件夹不存在: {folder_path}")
            return
            
        url = QUrl.fromLocalFile(folder_path)
        if not QDesktopServices.openUrl(url):
            QMessageBox.warning(self, "警告", f"无法打开文件夹: {folder_path}")

    def get_fail_csv(self):
        log_path_str = self.textEdit_logpath.toPlainText()
        if log_path_str ==  "":
            QMessageBox.critical(self, "错误", f"文件夹不存在：\n{log_path_str}")
            return False
        log_path = Path(log_path_str)
        print(log_path)
        if not log_path.is_dir():
            QMessageBox.critical(self, "错误", f"文件夹不存在：\n{log_path_str}")
            return False

        # 1. 先收集所有符合条件的文件路径（减少IO操作次数）
        records_files_found = list(log_path.rglob('records.csv'))
        if not records_files_found:
            QMessageBox.information(self, "提示", "未找到任何records.csv文件")
            return

        # 2. 批量检查已处理文件（减少数据库查询次数）
        file_paths = [str(fp) for fp in records_files_found]
        unprocessed_files = self.test_data.get_unprocessed_files(file_paths)
        
        if not unprocessed_files:
            QMessageBox.information(self, "提示", "没有需要处理的新文件")
            return

        # 3. 批量处理文件（使用线程池加速）
        from concurrent.futures import ThreadPoolExecutor, as_completed
        # 1. 创建线程安全的队列，存储所有处理后的数据（线程安全，无需额外加锁）
        batch_data_queue = queue.Queue()
        processed_count = 0
        max_workers = min(8, os.cpu_count() + 1)

        # 2. 线程池处理文件：只解析数据，存入队列
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务（传递队列给子线程）
            futures = {
                executor.submit(self.process_single_file, fp, batch_data_queue): fp 
                for fp in unprocessed_files
            }
            
            # 处理结果（统计成功处理的文件数）
            for future in as_completed(futures):
                try:
                    if future.result():
                        processed_count += 1
                except Exception as e:
                    print(f"处理文件失败 {futures[future]}: {str(e)}")

        # 3. 所有线程完成后，批量提取队列中的数据
        batch_data = []
        while not batch_data_queue.empty():
            batch_data.append(batch_data_queue.get())  # 每个元素是 (df_single, processed_file_path)

        # 4. 批量插入数据库（调用 TestData 的批量插入接口）
        if batch_data:
            try:
                self.test_data.batch_insert_test_data(batch_data)
                print(f"\n🎉 批量插入成功！共插入 {len(batch_data)} 组数据。")
            except Exception as e:
                QMessageBox.critical(self.textEdit_logpath, "批量插入失败", f"数据库批量插入出错：\n{str(e)}")
                return False

        # 5. 更新 UI
        self.update_table_fail()
        QMessageBox.information(self, "成功", 
                              f"处理完成！\n共扫描 {len(records_files_found)} 个文件，\n其中 {processed_count} 个为新文件并已成功处理。")


    def process_single_file(self, file_path, data_queue):
        """
        处理单个文件的逻辑，供线程池调用
        只解析数据，存入线程安全队列，不直接插入数据库
        :param file_path: 待处理文件路径
        :param data_queue: 线程安全的队列，用于存储处理后的数据
        :return: 是否处理成功（True/False）
        """
        try:
            fp = Path(file_path)
            # 快速检查：跳过空文件
            if fp.stat().st_size == 0:
                print(f"ℹ️ 跳过空文件：{file_path}")
                return False

            if self.test_data.is_file_processed(file_path):
                print(f"⚠️ 文件已经被存储不可以再存储")
                return False
                
            # 解析文件（原有逻辑不变）
            df_single, processed_file_path = self.test_data.parse_file(fp)
            
            # 过滤空数据
            if df_single.empty:
                print(f"ℹ️ 文件 {file_path} 解析后为空，跳过")
                return False
                
            # 将数据存入队列（线程安全）
            data_queue.put( (df_single, processed_file_path) )
            # print(f"✅ 成功解析文件：{file_path}（数据行数：{len(df_single)}）")
            return True
        except Exception as e:
            print(f"❌ 处理文件 {file_path} 失败: {str(e)}")
            return False

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = failInfoWindow()
    window.show()
    sys.exit(app.exec())