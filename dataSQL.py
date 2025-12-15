import sqlite3
import re,json,sys,os
from pathlib import Path
from typing import Tuple  # 优化类型提示
import pandas as pd
from typing import Optional
from datetime import datetime
import hashlib

if getattr(sys, 'frozen', False):
    # 已打包状态：获取可执行文件所在路径
    app_path = os.path.dirname(sys.executable)
else:
    # 未打包状态：获取当前Python文件所在路径
    app_path = os.path.dirname(os.path.abspath(__file__))

# 配置文件路径
CONFIG_PATH = os.path.join(os.path.join(app_path,"config"), 'config.json')

def calculate_file_md5(file_path: str, chunk_size: int = 4096) -> str:
    """
    计算文件的MD5值（用于文件内容唯一性校验）
    :param file_path: 文件路径
    :param chunk_size: 读取文件的块大小（默认4KB，避免大文件占用过多内存）
    :return: 文件的MD5十六进制字符串
    """
    md5_hash = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            # 分块读取文件（适配大文件）
            while chunk := f.read(chunk_size):
                md5_hash.update(chunk)
        return md5_hash.hexdigest()  # 返回32位MD5字符串
    except Exception as e:
        print(f"❌ 计算文件MD5失败：{file_path}，错误：{str(e)}")
        return ""

def convert_time_format(time_input):
    """
    将 A/B/C 三种时间格式统一转换为 C 格式 (2025-6-18 16:24:34)
    
    支持输入类型：字符串、pandas.Timestamp、datetime.datetime
    支持格式：
    A 格式: Jun 18 2025 4:24:34.3390 PM（英文月份+12小时制+毫秒）
    B 格式: 6月 18 2025 4:24:34.3390 上午（中文月份+12小时制+毫秒）
    C 格式: 2025-06-18 16:36:40.449000（标准格式+毫秒）
    
    参数:
        time_input: 输入的时间（字符串/Timestamp/datetime 对象）
        
    返回:
        str: 转换后的 C 格式时间字符串（无毫秒）
    """
    try:
        # 第一步：统一将输入转为字符串（处理 Timestamp/datetime 类型）
        if isinstance(time_input, (pd.Timestamp, datetime)):
            time_str = time_input.strftime('%Y-%m-%d %H:%M:%S.%f')  # 转为带毫秒的字符串
        else:
            # 若为字符串，先去除首尾空格
            time_str = str(time_input).strip()
        
        # 状态1：处理 C 格式（标准格式 + 毫秒，如 2025-06-18 16:36:40.449000）
        # 匹配规则：YYYY-MM-DD HH:MM:SS.xxx（毫秒部分可选）
        c_pattern = r'^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(\.\d+)?$'
        if re.match(c_pattern, time_str):
            # 截取毫秒前的字符串，再解析格式化（确保格式统一）
            time_no_ms = time_str.split('.')[0]
            dt = datetime.strptime(time_no_ms, '%Y-%m-%d %H:%M:%S')
            return dt.strftime('%Y-%-m-%-d %H:%M:%S')  # %-m 去除月份前的0（如 06→6）
        
        # 状态2：处理 A 格式（英文月份 + 12小时制 + 毫秒）
        english_months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", 
                          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        if any(month in time_str for month in english_months):
            # 移除微秒部分（.后面的数字+可选空格）
            time_str = re.sub(r'\.\d+\s*', ' ', time_str).strip()
            # 解析英文格式（%b=英文缩写月，%I=12小时制，%p=AM/PM）
            dt = datetime.strptime(time_str, '%b %d %Y %I:%M:%S %p')
            return dt.strftime('%Y-%-m-%-d %H:%M:%S')
        
        # 状态3：处理 B 格式（中文月份 + 12小时制 + 毫秒）
        elif '月' in time_str and ('上午' in time_str or '下午' in time_str):
            # 中文月份映射（1月→1，12月→12）
            month_map = {f"{i}月": str(i) for i in range(1, 13)}
            for cn_month, num in month_map.items():
                time_str = time_str.replace(cn_month, num)
            
            # 替换上午/下午为英文（适配 strptime 的 %p）
            time_str = time_str.replace('上午', 'AM').replace('下午', 'PM')
            # 移除微秒部分
            time_str = re.sub(r'\.\d+\s*', ' ', time_str).strip()
            
            # 解析中文转换后的格式（%m=数字月，%I=12小时制，%p=AM/PM）
            dt = datetime.strptime(time_str, '%m %d %Y %I:%M:%S %p')
            return dt.strftime('%Y-%-m-%-d %H:%M:%S')
        
        # 所有格式不匹配时，尝试直接解析为datetime（兜底）
        try:
            dt = datetime.strptime(time_str.split('.')[0], '%Y-%m-%d %H:%M:%S')
            print("兜底解析为 C 格式")
            return dt.strftime('%Y-%-m-%-d %H:%M:%S')
        except:
            print("格式不匹配，返回原始字符串")
            return time_str
    
    except Exception as e:
        print(f"时间格式转换错误: {e}")
        return str(time_input)  # 异常时返回原始输入的字符串形式


class TestData(object):
    def __init__(self,DB_PATH):
        super(TestData, self).__init__()
        self.DB_PATH = DB_PATH
        self.init_db()

    def parse_file(self, file_path: Path) -> Tuple[pd.DataFrame, str]:
        """
        解析测试记录CSV文件（适配实际文件结构）
        返回：解析后的DataFrame（空DataFrame表示失败）、文件路径字符串
        """
        try:
            # 1. 读取CSV，适配实际列名和数据类型
            df = pd.read_csv(
                file_path,
                parse_dates=['startTime', 'stopTime'],  # 解析实际时间列
                on_bad_lines='skip',  # 跳过异常行（如格式错误）
                dtype={
                    'measurementValue': float,  # 测试值（对应原函数test_value）
                    'attributeValue': str,  # 关键属性值（如SN、产品名，存于attributeValue列）
                    'testName': str,  # 测试名称（允许空，用str避免自动设为float）
                    'status': str,  # 测试状态（成功/失败，允许空）
                    'measurementUnits': str  # 测试单位（如V、A，允许空）
                }
            )

            # 2. 校验必要列（基于文件实际列名，确保核心数据不缺失）
            required_cols = [
                'attributeName',  # 属性名称（用于区分SN、产品名等）
                'attributeValue', # 属性值（存储关键标识）
                'measurementValue',  # 测试数值（核心数据）
                'startTime',  # 测试开始时间
                'status'  # 测试结果状态
            ]
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                raise ValueError(f"缺失必要列：{missing_cols} → 需包含{required_cols}")

            # 3. 数据预处理（可选，提升数据可用性）
            # 3.1 填充空值（避免后续分析报错）
            df['testName'] = df['testName'].fillna('未命名测试')  # 测试名空值填充
            df['status'] = df['status'].fillna('未知状态')  # 状态空值填充
            df['upperLimit'] = df['upperLimit'].fillna('未知上限')
            df['lowerLimit'] = df['lowerLimit'].fillna('未知下限')
            # 3.2 转换时间格式（统一为"YYYY-MM-DD HH:MM:SS"）
            # df['startTime'] = df['startTime'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
            # df['stopTime'] = df['stopTime'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')

            # print(f"✅ 解析成功 {file_path.name} → 有效行数：{len(df)}")
            return df, str(file_path)

        except Exception as e:
            print(f"❌ 解析失败 {file_path.name}：{str(e)}")
            return pd.DataFrame(), str(file_path)

    def init_db(self):
        """初始化数据库表（若不存在则创建）"""
        conn = sqlite3.connect(self.DB_PATH)#打开数据库的连接
        cursor = conn.cursor()#数据库的 “工具”
        # 测试数据表：存储单次测试的所有测试项（单文件对应多行记录）
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS test_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slot_id TEXT NOT NULL,  --产品测试通道号
            sn TEXT NOT NULL,  --产品SN
            test_time DATETIME NOT NULL,  -- 测试时间
            test_item TEXT NOT NULL,  -- 测试项（如value1、value2）
            test_value TEXT NOT NULL,  -- 测试值
            test_usl TEXT NOT NULL,  -- 测试上限
            test_lsl TEXT NOT NULL,  -- 测试下限
            test_result TEXT NOT NULL,  -- 测试结果（PASS/FAIL）
            file_path TEXT NOT NULL,  -- 源文件路径（避免重复入库）
            file_md5 TEXT NOT NULL,  -- 源文件md5值
            create_time DATETIME DEFAULT CURRENT_TIMESTAMP  -- 数据入库时间
        )
        ''')
        # 创建索引：加速按SN、测试项、时间查询
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sn ON test_records(sn)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_test_item ON test_records(test_item)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_test_time ON test_records(test_time)')
        conn.commit()
        conn.close()
        print(f"✅ 数据库初始化完成（文件路径：{self.DB_PATH}）")

    #检查文件是否被处理，文件数据是否被加载到数据库里，文件夹地址和文件md5值，两个条件判断
    def is_file_processed(self,file_path: str) -> bool:
        conn = None
        current_file_md5 = calculate_file_md5(file_path)
        if not current_file_md5:
            print(f"⚠️ 文件MD5计算失败，无法校验是否已入库：{file_path}")
            return False  # 计算失败时不视为已入库（避免误判）
        try:
            """判断文件是否已入库（避免重复）"""
            conn = sqlite3.connect(self.DB_PATH)
            cursor = conn.cursor()
            # 双重查询：匹配路径 或 匹配MD5值（两种情况都视为可能已入库）
            cursor.execute('''
                SELECT file_path, file_md5 FROM test_records  -- 字段顺序：路径在前，MD5在后
                WHERE file_path = ? OR file_md5 = ?
                LIMIT 1  -- 只要找到一条匹配记录即可
            ''', (file_path, current_file_md5))
            
            result = cursor.fetchone()
            if not result:
                # 路径和MD5都无匹配 → 未入库
                return False
            stored_file_path = result[0]  # 第一个字段：file_path
            stored_md5 = result[1]        # 第二个字段：file_md5
            if stored_md5 == current_file_md5 or stored_file_path == file_path:
                return True
            else:
                return False
        except sqlite3.Error as db_err:
            print(f"❌ 数据库校验失败：{str(db_err)}")
            return False
        except IndexError as e:
            print(f"❌ 结果解析错误（字段查询顺序错误）：{str(e)}")
            return False
        finally:
            if conn:
                conn.close()

    #传递pd参数的某一行，测试名有两种情况。
    def get_test_name(self,row: pd.Series) -> str:
        test_name = "未知测试名"
        attribute_name = row.get('attributeName', "")
        if pd.notna(attribute_name) and attribute_name != "":
            test_name = str(attribute_name).strip()
        else:
            test_name_parts = []
            for col in ['testName', 'subTestName', 'subSubTestName']:
                val = row.get(col, "")
                if pd.notna(val) and str(val).strip() != "":
                    test_name_parts.append(str(val).strip())
            if test_name_parts:
                test_name = "_".join(test_name_parts)
        return test_name

    #传递pd参数的某一行，测试值有两种情况，字符串或数字
    def get_test_value(self,row: pd.Series) -> str:
        attribute_value = row.get('attributeValue', "")
        if pd.notna(attribute_value) and str(attribute_value).strip():
            return str(attribute_value).strip()
        measurement_value = row.get('measurementValue', None)
        if pd.notna(measurement_value):
            return str(measurement_value).strip()
        return "没值"

    #单笔数据插入数据库中，是监控时候，当监控文件夹的文件出现新的测试数据时候使用
    def insert_test_data(self,df: pd.DataFrame, file_path: str) -> None:
        if self.is_file_processed(file_path):
            print(f"⚠️ 文件已经被存储不可以再存储")
            return

        self.slot_id_test_name = "ID"
        try:
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                config = json.load(f)
                if "slot_id_test_name" in config:
                    self.slot_id_test_name = config["slot_id_test_name"]
                else:
                    self.slot_id_test_name = "ID"
        finally:
            # print("self.slot_id_test_name",self.slot_id_test_name)
            pass

        df,device_sn,test_time,slotId = self.handleDF(df)

        current_file_md5 = calculate_file_md5(file_path)

        # 5. 准备批量插入数据
        data_tuples = [
            (
                slotId,
                device_sn,
                test_time,
                self.get_test_name(row),
                self.get_test_value(row),
                row['upperLimit'],
                row['lowerLimit'],
                row['status'],
                file_path,
                current_file_md5
            )
            for _, row in df.iterrows()
        ]

        # 6. 批量插入数据库
        if not data_tuples:
            print(f"⚠️ 无有效测试记录：文件={Path(file_path).name}，SN={device_sn}")
            return

        try:
            conn = sqlite3.connect(self.DB_PATH)
            cursor = conn.cursor()
            # print(data_tuples)
            cursor.executemany('''
                INSERT INTO test_records (slot_id, sn, test_time, test_item, test_value, test_usl, test_lsl, test_result, file_path, file_md5)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', data_tuples)
            conn.commit()
            # print(f"✅ 数据入库成功：SN={device_sn}，测试项数={len(data_tuples)}，文件={Path(file_path).name}")
        except sqlite3.Error as db_err:
            conn.rollback()
            print(f"❌ 数据库插入失败：SN={device_sn}，错误={str(db_err)}")
        finally:
            if conn:
                conn.close()

    def query_test_data(
        self,
        sn: Optional[str] = None,
        test_item: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None
    ) -> pd.DataFrame:
        """按需查询测试数据（支持按SN、测试项、时间范围筛选）"""
        conn = sqlite3.connect(self.DB_PATH)
        # 构建查询条件
        query = "SELECT sn, test_time, test_item, test_value FROM test_records WHERE 1=1"
        params = []
        if sn:
            query += " AND sn = ?"
            params.append(sn)
        if test_item:
            query += " AND test_item = ?"
            params.append(test_item)
        if start_time:
            query += " AND test_time >= ?"
            params.append(start_time)
        if end_time:
            query += " AND test_time <= ?"
            params.append(end_time)
        # 执行查询并解析时间列
        df = pd.read_sql(query, conn, params=params, parse_dates=['test_time'])
        conn.close()
        return df.sort_values(['sn', 'test_time'])  # 按SN和时间排序

    def parse_exclude_str(self, exclude_str):
        """
        解析排除项字符串为列表（支持逗号/分号/空格分隔）
        :param exclude_str: 排除项字符串，如 "CHECK_STATION_SECURITY,OrphanedRequiredLimits;OrphanedRecords"
        :return: 去重后的排除项列表
        """
        if not exclude_str or exclude_str.strip() == "":
            return []
        # 支持多种分隔符：逗号、分号、空格、换行
        separators = [',', ';', ' ', '\n', '\t']
        exclude_list = exclude_str.strip()
        for sep in separators:
            exclude_list = exclude_list.replace(sep, ',')
        # 拆分+去重+过滤空值
        exclude_list = [item.strip() for item in exclude_list.split(',') if item.strip()]
        return list(set(exclude_list))

    def get_fail_data(self, 
                      sn_filter="", 
                      test_item_exclude_str="",
                      slot_id_exclude_str="",
                      start_time_str="",
                      end_time_str=""):
        """
        获取测试失败的数据，支持多维度筛选
        :param sn_filter: SN筛选关键词（模糊匹配），默认为空不筛选
        :param test_item_exclude_str: 排除的test_item字符串（多值用逗号/分号/空格分隔）
        :param slot_id_exclude_str: 排除的slot_id字符串（多值用逗号/分号/空格分隔）
        :param start_datetime: 开始时间（QDateTime对象），None则不限制
        :param end_datetime: 结束时间（QDateTime对象），None则不限制
        :return: 筛选后的失败数据DataFrame
        """
        conn = sqlite3.connect(self.DB_PATH)
        try:
            # ========== 1. 初始化基础条件和参数 ==========
            base_conditions = [
                "test_result = 'FAIL'",
                "test_time != '未知时间'"  # 排除无效时间
            ]
            query_params = []

            # ========== 2. 解析test_item排除条件 ==========
            exclude_test_items = self.parse_exclude_str(test_item_exclude_str)
            if exclude_test_items != "" :
                # 生成 test_item NOT IN (?, ?, ...) 条件
                placeholders = ', '.join(['?'] * len(exclude_test_items))
                base_conditions.append(f"test_item NOT IN ({placeholders})")
                query_params.extend(exclude_test_items)

            # ========== 3. 解析slot_id排除条件 ==========
            exclude_slot_ids = self.parse_exclude_str(slot_id_exclude_str)
            if exclude_slot_ids:
                # 注意：slot_id如果是数字类型，需确保传入的是数字字符串
                placeholders = ', '.join(['?'] * len(exclude_slot_ids))
                base_conditions.append(f"slot_id NOT IN ({placeholders})")
                query_params.extend(exclude_slot_ids)

            # ========== 4. 时间范围筛选（test_time） ==========
            if start_time_str != "":
                base_conditions.append("test_time >= ?")
                query_params.append(start_time_str)

            if end_time_str != "":
                base_conditions.append("test_time <= ?")
                query_params.append(end_time_str)

            # ========== 5. SN模糊筛选 ==========
            if sn_filter and sn_filter.strip():
                base_conditions.append("sn LIKE ?")
                query_params.append(f'%{sn_filter.strip()}%')

            # ========== 6. 组装查询语句 ==========
            fail_query = f"""
            SELECT slot_id, sn, test_time, test_item, test_value, test_usl, test_lsl, test_result, file_path 
            FROM test_records 
            WHERE {' AND '.join(base_conditions)}
            ORDER BY test_time DESC
            """

            # print("原始SQL：", fail_query)
            # print("参数列表：", query_params)
            # 手动替换占位符（仅调试，无需依赖sqlite3方法）
            # temp_sql = fail_query
            # for param in query_params:
            #     temp_sql = temp_sql.replace('?', f"'{param}'", 1)
            # print("拼接后SQL：", temp_sql)

            # ========== 7. 执行查询 ==========
            fail_df = pd.read_sql(
                fail_query,
                conn,
                params=query_params,
                parse_dates=['test_time']  # 自动解析为datetime类型
            )

        finally:
            conn.close()

        return fail_df

    # def get_fail_data(self, sn_filter=""):
    #     """
    #     获取测试失败的数据，排除test_item为CHECK_STATION_SECURITY和OrphanedRequiredLimits的记录
    #     :param sn_filter: SN筛选关键词（模糊匹配），默认为空不筛选
    #     :return: 筛选后的失败数据DataFrame
    #     """
    #     conn = sqlite3.connect(self.DB_PATH)
    #     try:
    #         # 基础WHERE条件：失败、时间非未知、排除指定test_item
    #         base_conditions = [
    #             "test_result = 'FAIL'",
    #             "test_time != '未知时间'",
    #             "test_time != 'OrphanedRequiredLimits'",
    #             "test_item != 'CHECK_STATION_SECURITY'"
    #         ]
    #         # 拼接SN筛选条件（如果有）
    #         if sn_filter:
    #             base_conditions.append("sn LIKE ?")
            
    #         # 组装完整查询语句
    #         fail_query = f"""
    #         SELECT slot_id, sn, test_time, test_item, test_value, test_usl, test_lsl, test_result, file_path 
    #         FROM test_records 
    #         WHERE {' AND '.join(base_conditions)}
    #         """
    #         # 设置查询参数（仅SN筛选时有值）
    #         fail_params = [f'%{sn_filter}%'] if sn_filter else []
            
    #         # 读取数据（解析test_time为日期类型）
    #         fail_df = pd.read_sql(
    #             fail_query, 
    #             conn, 
    #             params=fail_params, 
    #             parse_dates=['test_time']
    #         )
    #     finally:
    #         # 确保连接无论是否异常都关闭
    #         conn.close()
        
    #     return fail_df

    #传入文件夹地址列表，判断表中的那些文件是否已经被处理过，返回没被处理文件地址列表
    def get_unprocessed_files(self, file_paths):
        """批量检查未处理文件，减少数据库查询次数"""
        if not file_paths:
            return []
            
        conn = sqlite3.connect(self.DB_PATH)
        cursor = conn.cursor()
        
        # 批量查询已存在的文件路径和MD5
        placeholders = ', '.join('?' for _ in file_paths)
        cursor.execute(f'''
            SELECT file_path, file_md5 FROM test_records 
            WHERE file_path IN ({placeholders})
        ''', file_paths)
        
        processed = set()
        for path, md5 in cursor.fetchall():
            processed.add(path)
        
        conn.close()
        
        # 计算未处理文件
        unprocessed = []
        for fp in file_paths:
            if fp not in processed:
                unprocessed.append(fp)
        
        return unprocessed
        
    #一个pd参数，代表的是records.csv，同一个csv，sn和通道号，测试结果和测试时间是一致的，单独解析出来
    def handleDF(self,df: pd.DataFrame):
        """将适配后解析的测试数据批量插入数据库"""
        # 1. 基础校验：DataFrame为空或无必要列，直接返回
        required_cols = ['attributeName', 'attributeValue', "testName","subTestName","subSubTestName","upperLimit","measurementValue","lowerLimit","measurementUnits",'startTime', "stopTime", 'status']
        if df.empty or not all(col in df.columns for col in required_cols):
            print(f"⚠️ 跳过入库：数据为空或缺失必要列（需包含{required_cols}）")
            return

        # 2. 提取SN
        sn_row = df[df['attributeName'] == 'PrimaryIdentity']
        device_sn: Optional[str] = sn_row['attributeValue'].iloc[0] if not sn_row.empty else "未知SN"
        if device_sn is None or device_sn == "":
            device_sn = "未知SN"

        # 3. 提取测试时间（统一用startTime，格式化为datetime）
        time_row = df[df['startTime'].notna() & (df['startTime'] != "")]
        test_time_str: Optional[str] = time_row['startTime'].iloc[0] if not time_row.empty else "2025-01-01 00:00:00"

        # print("test_time_str",test_time_str)
        test_time = convert_time_format(test_time_str)

        # 4. 处理测试结果（统一为PASS/FAIL）
        df['status'] = df['status'].fillna('PASS')
        df['status'] = df['status'].apply(lambda x: 'FAIL' if str(x).upper() in ['FAIL', 'ERROR'] else 'PASS')

        # 6. 提取通道号
        slotId_row = df[df['subSubTestName'] == self.slot_id_test_name]
        slotId: Optional[str] = slotId_row['measurementValue'].iloc[0] if not slotId_row.empty else "未知通道号"
        if slotId is None or slotId == "":
            slotId = "未知通道号"

        df = df.fillna("")  # 把所有 NaN 替换为空字符串（根据字段类型调整，比如数值型用 0）
        return df,device_sn,test_time,slotId

    #批量插入数据到数据库中，是遍历某个文件夹得到的数据
    def batch_insert_test_data(self, batch_data):
        data_tuples_all = []

        self.slot_id_test_name = "ID"
        try:
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                config = json.load(f)
                if "slot_id_test_name" in config:
                    self.slot_id_test_name = config["slot_id_test_name"]
                else:
                    self.slot_id_test_name = "ID"
        finally:
            # print("self.slot_id_test_name",self.slot_id_test_name)
            pass

        for df, file_path in batch_data:
            current_file_md5 = calculate_file_md5(file_path)
            df,device_sn,test_time,slotId = self.handleDF(df)
            data_tuples = [
                (
                    str(slotId),
                    str(device_sn) if device_sn else "",  # 确保是字符串，避免 None
                    str(test_time) if test_time else "",  # 确保是字符串
                    str(self.get_test_name(row)) if self.get_test_name(row) else "",
                    str(self.get_test_value(row)) if self.get_test_value(row) else "",  # 数值型可保留原类型
                    row['upperLimit'],
                    row['lowerLimit'],
                    str(row['status']) if 'status' in row and row['status'] is not None else "",
                    str(file_path) if file_path else "",
                    str(current_file_md5) if current_file_md5 else ""
                )
                for _, row in df.iterrows()
            ]
            data_tuples_all.extend(data_tuples)


        try:
            conn = sqlite3.connect(self.DB_PATH)
            cursor = conn.cursor()
            total = len(data_tuples_all)
            print(f"待插入数据总条数：{total}")
            batch_size = 2000
            for i in range(0, total, batch_size):
                batch = data_tuples_all[i:i+batch_size]
                # print(batch)
                cursor.executemany('''
                    INSERT INTO test_records (slot_id, sn, test_time, test_item, test_value, test_usl, test_lsl, test_result, file_path, file_md5)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', batch)
                print(f"已插入第 {i//batch_size + 1} 批，累计 {min(i+batch_size, total)}/{total} 条")
            conn.commit()
            print(f"✅ 数据入库成功，测试项数={len(data_tuples_all)}")
            # df_all = self.query_test_data()
            # sn_list = df_all['sn'].unique()  # 所有产品SN
            # print(sn_list)
        except sqlite3.Error as db_err:
            conn.rollback()
        except Exception as e:
            print(e)
        finally:
            if conn:
                conn.close()

if __name__ == "__main__":
    DB_PATH = Path("./test_data.db")
    TestData = TestData(DB_PATH)

    single_file = Path("/Users/gdlocal/Library/Logs/Atlas/unit-archive/DWHHH70AA3B00013F3/20250618_16-37-05.625-4566D3/system/records.csv")
    df_single, file_path = TestData.parse_file(single_file)

    # 查看解析结果（前3行核心数据）
    if not df_single.empty:
        print("\n=== 解析结果预览（核心列）===")
        preview_cols = ['attributeName', 'attributeValue', "testName","subTestName","subSubTestName","upperLimit","measurementValue","lowerLimit","measurementUnits",'startTime', "stopTime", 'status']
        print(df_single[preview_cols].head(30))

    # TestData.init_db()

    TestData.insert_test_data(df_single,file_path)

    df_all = TestData.query_test_data()
    test_items = df_all['test_item'].unique()  # 所有测试项（如value1、value2、value3、value4）
    print(test_items)
    sn_list = df_all['sn'].unique()  # 所有产品SN
    print(sn_list)

    test_data1 = TestData.query_test_data(test_item="Connectivity_ShortTest_RX1_P_E85_B11_TO_DOCK_P17")
    print(test_data1)
