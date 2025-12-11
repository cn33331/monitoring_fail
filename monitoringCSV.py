from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dataSQL import TestData

# 自定义事件处理器（继承FileSystemEventHandler，重写需要的事件方法）
class BasicFileHandler(FileSystemEventHandler):
    def __init__(self, update_callback,on_dir_deleted_callback):
        self.update_callback = update_callback  # UI更新回调函数
        self.TestData = None  # 延迟初始化，在start中设置
        self.on_dir_deleted_callback = on_dir_deleted_callback  # 目录删除回调
        self.MONITOR_DIR = None

    def start(self,MONITOR_DIR,test_data):
        self.MONITOR_DIR = MONITOR_DIR
        self.TestData = test_data
        self.observer = Observer()
        # 配置监控：监听目标文件夹，递归监控所有子文件夹（recursive=True）
        self.observer.schedule(
            self,
            path=str(self.MONITOR_DIR),
            recursive=True
        )

        # 启动监控
        self.observer.start()
        print(f"📋 开始监控文件夹：{self.MONITOR_DIR}")
        print(f"💡 提示：在 {self.MONITOR_DIR} 下创建/修改/删除文件，查看输出")
        return self.observer
    # 当文件被创建时触发
    def on_created(self, event):
        file_path = Path(event.src_path)
        # 过滤目标文件：CSV + 文件名是records.csv + 未处理过
        if (not event.is_directory 
            and file_path.name == "records.csv"
            and not self.TestData.is_file_processed(str(file_path))):
            
            print(f"\n🔍 检测到新增测试文件：{file_path.name}")
            df_single, file_path = self.TestData.parse_file(file_path)
            self.TestData.insert_test_data(df_single,file_path)
            self.update_callback()  # 通知UI更新
            print("触发UI更新回调")

    # 当文件被修改时触发（注意：某些编辑器保存可能触发多次）
    # def on_modified(self, event):
    #     if not event.is_directory:
    #         print(f"🔄 文件修改：{event.src_path}")

    def on_deleted(self, event):
        # 只处理目录删除事件，且删除的是监控的根目录（不是子目录）
        if event.is_directory:
            deleted_dir = Path(event.src_path).absolute()
            # 判断删除的是否是我们监控的根目录（避免子目录删除误触发）
            if deleted_dir == self.MONITOR_DIR:
                print(f"\n⚠️  警告：监控目录已被删除：{deleted_dir}")
                # 触发使用者传入的回调，让其自定义处理逻辑
                if self.on_dir_deleted_callback:
                    try:
                        self.on_dir_deleted_callback()
                    except Exception as e:
                        print(f"❌ 目录删除回调执行失败：{str(e)}")
                else:
                    print("ℹ️  未设置目录删除回调，跳过处理")

    # # 当文件/文件夹被移动时触发
    # def on_moved(self, event):
    #     print(f"➡️  移动：{event.src_path} -> {event.dest_path}")


def test():
    print(test)

def test2():
    print(test)


if __name__ == "__main__":

    MONITOR_DIR = Path("/Users/gdlocal/Library/Logs/Atlas/unit-archive")
    MONITOR_DIR.mkdir(exist_ok=True)  # 确保文件夹存在
    DB_PATH = Path("./test_data.db")
    TestData = TestData(DB_PATH)
    # 创建事件处理器和监控器
    event_handler = BasicFileHandler(test,test2)
    observer = event_handler.start(MONITOR_DIR,TestData)
    try:
        # 主线程阻塞，保持监控运行（按 Ctrl+C 停止）
        while True:
            observer.join(1)  # 每1秒检查一次，避免CPU占用过高
    except KeyboardInterrupt:
        # 手动停止监控，清理资源
        observer.stop()
        print("\n🛑 监控已停止")
    observer.join()  # 等待监控线程结束