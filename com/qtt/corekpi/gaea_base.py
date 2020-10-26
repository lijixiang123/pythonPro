# -*- coding: utf-8 -*-
import requests
import time, json
import datetime
import dataclasses
from dataclasses import dataclass
import pandas as pd
from retrying import retry
from sqlalchemy.engine import create_engine
import traceback
import numpy as np
@dataclass
class GaeaBase:
    # 接口说明文档 http://km.qutoutiao.net/pages/viewpage.action?pageId=91469958
    #测试环境接口
    # qe_submit_api: str = "http://test-data.qttcs3.cn/newqe/api/job/submit"
    # qe_sync_execute_api: str = "http://test-data.qttcs3.cn/newqe/api/job/execute"
    # qe_task_status_api: str = "http://test-data.qttcs3.cn/newqe/api/job/status/{task_id}"
    # qe_task_result_api: str = "http://test-data.qttcs3.cn/newqe/api/job/result/{task_id}"
    # gaea_call_back_api: str = "http://indicator-test.qttcs3.cn/tableau/qw/send"
    # 生产环境接口
    qe_submit_api: str = "http://inner-query-editor.1sapp.com/api/job/submit"
    qe_sync_execute_api: str = "http://inner-query-editor.1sapp.com/api/job/execute"
    qe_task_status_api: str = "http://inner-query-editor.1sapp.com/api/job/status/{task_id}"
    qe_task_result_api: str = "http://inner-query-editor.1sapp.com/api/job/result/{task_id}"
    gaea_call_back_api: str = "http://dataplatform.qutoutiao.net/tableau/qw/send"
    task_ids: list = dataclasses.field(default_factory=list)
    task_dict: dict = dataclasses.field(default_factory=dict)
    robot_dict = {
        "dc_msg_robot_alert": {"name": "盖亚晨报预警", "templateKey": "3aaca6ceee29d3cffebe45292677cfad"},
        "dc_msg_robot0": {"name": "数据中心-盖亚报表平台-测试机器人", "templateKey": "8751e3ccd69636107a466b6f7e7c2090"},
        #朱琦琦负责
        "dc_msg_robot": {"name": "数据中心-盖亚报表平台-01", "templateKey": "29ff5b9c73f656b46a86cf12c94fb32e"},
        "dc_msg_robot2": {"name": "数据中心-盖亚报表平台-02", "templateKey": "cda2260db12e6a28f8827a03bbbe02ab"},
        "dc_msg_robot3": {"name": "数据中心-盖亚报表平台-03", "templateKey": "a6a8b0ddc175636a3591c27e1b48d899"},
        #苏珊珊，黄锦瑞负责
        "dc_msg_robot4": {"name": "数据中心-盖亚报表平台-04", "templateKey": "a81d5a14ba4fbed28685c0848db9fac4"},
        "dc_msg_robot4_pre": {"name": "数据中心-盖亚报表平台-04-预发 ", "templateKey": "75b4ddb156ab3d9731aee347feff050a"},
        #李飞洋、吴浩 趣键盘
        "dc_msg_robot5": {"name": "数据中心-盖亚报表平台-05", "templateKey": "fec074a615a836220ed2475d8245528f"},
        #林淑娟 RZ各APP晨报
        "dc_msg_robot6": {"name": "数据中心-盖亚报表平台-06", "templateKey": "25fbc7e2d026aa01cbaf20c4d6e60d3c"},
        #高子惠、蔡乔伊
        "dc_msg_robot7": {"name": "数据中心-盖亚报表平台-07", "templateKey": "e3107560140ace92106fdc85ed31d9ac"},
        #尹泽元
        "dc_msg_robot8": {"name": "数据中心-盖亚报表平台-08", "templateKey": "56407809635046fd27a4269290e53f79"},
        #邓华平
        "dc_msg_robot9": {"name": "数据中心-盖亚报表平台-09", "templateKey": "c904fb7578bba52cf40139a1f029643d"},
        #王乙平
        "dc_msg_robot10": {"name": "数据中心-盖亚报表平台-10", "templateKey": "bf8ceeb85da06850082141f74d535768"},
        "dc_msg_robot_finance": {"name": "数据中心-财务日报", "templateKey": "08348c94d8b75153b3588c0b9bb72452"},
        #老机器人key
        "dp_report_test": {"name": "业务核心早报数据测试", "templateKey": "0e846146-64c8-4ced-aba9-f6bd7a50da4e"},
        "dp_report_push": {"name": "Push业务核心早报数据", "templateKey": "3b36df4c-a8dd-4115-af6d-d2dd478988f8"},
        #日活晨报数据【TUID】、内容推荐晨报、互动体验晨报、DD晨报数据【TUID】、米读晨报数据【TUID】、老铁晨报数据【TUID】、RZ晨报数据【TUID】
        "dp_report_qtt": {"name": "日活晨报数据【TUID】", "templateKey": "04257a56-c4d4-4feb-992b-c5d2bd088d0e"},
        "dp_report_dd": {"name": "DD的业务群", "templateKey": "06f89cdd-1ca7-4e12-812d-7320aeef9b8a"},
        "dp_report_midu": {"name": "midu的业务群", "templateKey": "93fa0c72-42f8-4635-aeea-627d638286a8"},
        "dp_report_rz_app": {"name": "RZ各APP的业务群", "templateKey": "89d71bb8-b103-4c16-b7d8-7dc2b3e8d591"},
        #新robot兼容老的
        "dp_report_qtt_new": {"name": "数据中心-业务核心早报", "templateKey": "f5897aca86ac018b31f48d1c9609b77d"},
        "dp_report_midu_new": {"name": "数据中心-米读增长早报", "templateKey": "0d1e650a34c2592ce727ce8312dcab49"},
        "dp_report_push_new": {"name": "Push业务核心早报数据", "templateKey": "6356cfaf18d02aae5f1cdfc5cd2bd829"},
        "dp_report_dd_new": {"name": "数据中心-DD增长早报", "templateKey": "892ba0460e2415add27b09e811992f21"},
        "dp_report_rz_app_new": {"name": "数据中心-RZ各APP早报", "templateKey": "c2b4a6bd07c8b0e74f2f5e054bbb000c"},
        #顾洪程
        "qxq_test": {"name": "数据中心-相亲直播-test", "templateKey": "0122a8341c799dfd85de6c290adddef2"},
        "qxq_prd": {"name": "数据中心-相亲直播", "templateKey": "fef90d966a565fce607492e40b3e210c"}

    }
    presto_engine = None
    '''
    format_type='placeholder' or 'order'
    '''
    def get_report_content(self, query_sql: str, template: str, warning_users, report_title='test', format_type='placeholder', type='markdown'):
        is_placeholder = format_type=='placeholder'
        try:
            result_df = self.get_df_from_presto(query_sql)
            if len(result_df) != 1:
                self.new_robot_alert('dc_msg_robot_alert', 'ERROR', f'{report_title} query result size not equal 1', warning_users)
                raise Exception("query result size not equal 1")
            columns = result_df.columns
            ind_param = {'report_title': report_title} if is_placeholder else [report_title]
            for index, row in result_df.iterrows():
                for c in columns:
                    print(f'{c}: {row[c]}')
                    if row[c] in [np.nan, None, 'null', 'NULL']:
                        self.new_robot_alert('dc_msg_robot_alert', 'ERROR', f'{report_title} 字段： {c} is null', warning_users)
                        #raise Exception(f"field: {c} is nan")
                        row[c] = 0
                    val = float(row[c])
                    if c.endswith(('_1ratio', '_7ratio')):
                        show_val = f'{row[c]}%'
                        if val != 0 and type=='markdown':
                            show_val = f'<font color="info">{row[c]}%</font>' if val < 0 else f'<font color="red">{row[c]}%</font>'
                        if is_placeholder:
                            ind_param[c] = show_val
                        else:
                            ind_param.append(show_val)
                    else:
                        val = row[c] if int(row[c])!=row[c] else int(row[c])
                        if is_placeholder:
                            ind_param[c] = val
                        else:
                            ind_param.append(val)
            print(ind_param)
            return template.format(**ind_param) if is_placeholder else template.format(*ind_param)
        except Exception as e:
            print(e)
            self.ERROR(" query SQL or format msg failed")
            traceback.print_exc()
            raise Exception(f"{report_title} get_report_content failed")

    '''
    format_type='placeholder' or 'order'
    '''
    def get_report_content_text(self, query_sql: str, template: str, warning_users, report_title='test', format_type='placeholder'):
        is_placeholder = format_type=='placeholder'
        try:
            result_df = self.get_df_from_presto(query_sql)
            if len(result_df) != 1:
                self.new_robot_alert('dc_msg_robot_alert', 'ERROR', f'{report_title} query result size not equal 1', warning_users)
                raise Exception("query result size not equal 1")
            columns = result_df.columns
            ind_param = {'report_title': report_title} if is_placeholder else [report_title]
            for index, row in result_df.iterrows():
                for c in columns:
                    print(f'{c}: {row[c]}')
                    if row[c] in [np.nan, None, 'null', 'NULL']:
                        self.new_robot_alert('dc_msg_robot_alert', 'ERROR', f'{report_title} 字段： {c} is null', warning_users)
                        raise Exception(f"field: {c} is nan")
                    val = float(row[c])
                    if c.endswith(('_1ratio', '_7ratio')):
                        show_val = f'{row[c]}%'
                        if is_placeholder:
                            ind_param[c] = show_val
                        else:
                            ind_param.append(show_val)
                    else:
                        val = row[c] if int(row[c])!=row[c] else int(row[c])
                        if is_placeholder:
                            ind_param[c] = val
                        else:
                            ind_param.append(val)
            print(ind_param)
            return template.format(**ind_param) if is_placeholder else template.format(*ind_param)
        except Exception as e:
            self.ERROR(" query SQL or format msg failed")
            traceback.print_exc()
            raise Exception(f"{report_title} get_report_content failed")
    def send_report_msg(self, robots: str, report_title: str, content: str, send_users=[], type='markdown'):
        try:
            message = self.msg_template.format(title=report_title, content=content, now_time=self.get_now_time())
            for robot in robots:
                self.new_robot_alert(robot, report_title, message, send_users, type)
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"{report_title} report send_msg failed")
    def alert_by_uids(self, subject, content, uids):
        url_alert = "http://data.qutoutiao.net/message/api/alert"
        headers = {
            'Content-Type': 'application/json',
            'Connection': 'close'
        }
        body = {
            "uids": uids,
            "types": "2",
            "subject": subject,
            "content": content
        }

        try:
            response = requests.post(url_alert, data=json.dumps(body), headers=headers).json()
            self.INFO(f'wechat alert result: {response}')
            self.INFO("=======wechat send message success=========")
        except Exception as e:
            self.ERROR(e)
            raise Exception(f"wechat send message failed")
        print('============ WECHAT ALERT ==========')
    '''
        新版接口，templateKey自助申请，http://rss.qutoutiao.net
    '''
    def new_robot_alert(self, robot: str, subject: str, content: str, sendUsers=[], type='markdown'):
        self.INFO("=======robot start send message=========")
        payload = {
            "templateKey": self.robot_dict[robot]['templateKey'],
            "message": content,
            "name": subject,
            "type": type,
            "sendUsers": sendUsers
        }
        self.INFO(f"message: {content}")
        try:
            response = requests.post("http://rss-api.qutoutiao.net/openapi/message", json=payload).json()
            self.INFO(f'qw send result: {response}')
            self.INFO("=======robot send message success=========")
        except Exception as e:
            self.ERROR(e)
            raise Exception(f"robot send message failed")
    '''
        旧版接口，key需要找尤凌飞
    '''
    def old_robot_alert(self, robot, subject, content):
        url_alert = "http://inner-data-message.1sapp.com/api/group_robot_alert"
        headers = {
            'Content-Type': 'application/json',
            'Connection': 'close'
        }
        body = {
            "content": content,
            "subject": subject,
            "key": self.robot_dict[robot]['templateKey']
        }
        self.INFO(f"message: {content}")
        try:
            response = requests.post(url_alert, data=json.dumps(body), headers=headers).json()
            self.INFO(f'qw send result: {response}')
            self.INFO("=======robot send message success=========")
        except Exception as e:
            self.ERROR(e)
            raise Exception(f"robot send message failed")
        print('============GROUP WECHAT ALERT ==========')
    def get_single_field_val_from_presto(self, sql: str):
        df = self.get_df_from_presto(sql)
        self.INFO(df)
        return df.iloc[0][0]
    @retry(stop_max_attempt_number=3, wait_fixed=5000)
    def get_df_from_presto(self, sql: str, host: str = 'dc-presto-prd.qttcs3.cn', port: int = 80, catalog: str = 'hive'):
        try:
            print(f'sql: {sql}')
            if self.presto_engine is None:
                self.INFO('presto_engine is None, now create_engine')
                self.presto_engine = create_engine(f'presto://{host}:{port}/{catalog}/')
            return pd.read_sql(sql, self.presto_engine)
        except Exception as e:
            self.ERROR(f"get_df_from_presto sql: {sql} failed")
            self.ERROR(e)
            traceback.print_exc()
    @retry(stop_max_attempt_number=1, wait_fixed=100000)
    def call_gaea_send_msg(self, tableau_views: [], text_msg: str, robot: dict, type ='text', sendUsers=[]):
        self.INFO(f"call_gaea_send_msg tableau_views: {tableau_views} , text_msg: {text_msg} start")
        post_data = {
            "tableauViews": tableau_views,
            "qwParam": {
                "robotName": robot['name'],
                "templateKey": robot['templateKey'],
                "type": type,
                "message": text_msg,
                "sendUsers": sendUsers
            }
        }
        response = requests.post(self.gaea_call_back_api, json=post_data)
        if response.content:
            result = response.json()
            self.INFO("call_gaea_send_msg response: {}".format(result))
            if result['status'] != 200 and result['data']:
                raise Exception(f"send tableau_views: {tableau_views} failed")
            else:
                self.INFO(f"call_gaea_send_msg tableau_views: {tableau_views} success.")
        else:
            raise Exception(f"send tableau_views: {tableau_views} failed")
    '''
    提交任务到QE，异步执行获取结果
    '''
    @retry(stop_max_attempt_number=20, wait_fixed=2000)
    def qe_task_submit(self, sql: str, task_name: str):
        post_data = {
            "engine": "presto",
            "userName": "point",
            "sql": sql,
            'taskName': task_name
        }
        response = requests.post(self.qe_submit_api, json=post_data).json()
        if response['errorCode'] != 0:
            raise Exception(f"{sql} task submit failed")
        else:
            self.task_ids.append(response['data'])
            self.task_dict[task_name] = response['data']
            self.INFO(f"{sql} task submit successful")
    '''
    请求QE同步获取结果，该接口针对userName有频次限制，重试请慎重
    (只适合小数据量快速返回[20s以内]的查询)
    
    errorCode:
    1000-任务繁忙
    1001-结果集太大了，数据被截断了，同步接口只适合少量数据返回的情况
    1002-请求的用户不存在
    1003-用户没有操作表的权限
    1004-不能识别的任务Id
    1006-任务执行异常
    1007-不能识别的执行引擎
    1008-不能识别的用户组
    1009-API call次数超过上限
    1010-API请求错误的入参
    
    return {
        "status": 200,
        "message": "ok",
        "data": [["185185786"]],
        "errorCode": 0,
        "columnsName": ["_col0"]
    }
    '''
    @retry(stop_max_attempt_number=20, wait_fixed=2000)
    def qe_task_sync_execute(self, userName: str, sql: str, taskName: str):
        post_data = {
            "engine": "presto",
            "taskName": taskName,
            "userName": userName,
            "user": "datacenter",
            "sql": sql
        }
        headers = {'Content-Type': 'application/json', 'Connection': 'close'}
        response = requests.post(url=self.qe_sync_execute_api, headers=headers, json=post_data, timeout=5).json()
        error_code = response['errorCode']
        message = response['message']
        if error_code != 0:
            self.ERROR(f"{sql} task execute failed, error_code: {error_code},  message: {message}")
            if error_code == 1000:
                raise Exception("task execute failed, will retry...")
        self.INFO(f"{sql} task execute successful")
        return response
    @retry(stop_max_attempt_number=20, wait_fixed=2000)
    def get_qe_task_ready(self, task_id: int) -> bool:
        api = self.qe_task_status_api.format(task_id=task_id)
        response = requests.get(api).json()
        if response['data'] == "FAILED":
            task_dict_reverse = {v: k for k, v in self.task_dict.items()}
            raise Exception(f"task {task_dict_reverse[task_id]} {response}failed")
        return True if response['data'] == "SUCCEEDED" else False
    def get_qe_task_result(self, task_id: int):
        api = self.qe_task_result_api.format(task_id=task_id)
        response = requests.get(api).json()
        response_file = response['data']
        return pd.read_csv(response_file, dtype=str)
    def get_qe_result(self, task_name):
        task_id = self.task_dict[task_name]
        while not self.get_qe_task_ready(task_id):
            time.sleep(5)
        result = self.get_qe_task_result(task_id)
        return result
    '''
        把对象(支持单个对象、list、set)转换成字典
    '''
    def class_to_dict(self, obj):
        is_list = obj.__class__ == [].__class__
        is_set = obj.__class__ == set().__class__
        if is_list or is_set:
            obj_arr = []
            for o in obj:
                # 把Object对象转换成Dict对象
                dict = {}
                dict.update(o.__dict__)
                obj_arr.append(dict)
            return obj_arr
        else:
            dict = {}
            dict.update(obj.__dict__)
            return dict
    def get_last_days(self, days: int = 0):
        return (datetime.datetime.now() - datetime.timedelta(days=days)).strftime("%Y-%m-%d")
    def get_some_day_ago(self, dateStr, days: int = 0):
        return (datetime.datetime.strptime(dateStr, "%Y-%m-%d") - datetime.timedelta(days=days)).strftime("%Y-%m-%d")
    def str2datetime(self, date_str: str):
        return datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    def get_now_time(self):
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    def get_month_day_ago(self, dateStr, days: int = 0):
        return (datetime.datetime.strptime(dateStr, "%Y-%m-%d") - datetime.timedelta(days=days)).strftime("%m月%d日")
    ############################################## LOGGER ##################################
    '''
        Loger
    '''
    def log(self, level, msg):
        msgstr = '%s,[%s] %s' % (datetime.datetime.now(), level, msg)
        print(msgstr)
    def INFO(self, msg):
        self.log('INFO', msg)
    def ERROR(self, msg):
        self.log('ERROR:', msg)
