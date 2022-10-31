#!/usr/bin/python3


import pymysql
import logging
import sys
import datetime
import time
from pathlib import Path
import os

format_log = "%(asctime)s %(message)s"  # 
logging.basicConfig(filename='logs/{}-handle.log'.format(datetime.datetime.today().date()), format=format_log, level=logging.INFO)


class DeleteData:
    def __init__(self, user_list=[], agent_list=[]):
        self._game_db = pymysql.connect(host='###', user='###', password='###',port=3306, charset="utf8")
        self._log_db = pymysql.connect(host='###', user='###', password='###',port=3306, charset="utf8")
        #self._delete_date = datetime.date.today()
        self._areaname_list = './areaname_list'


    def get_db_connettions(self):
        self.cursor_game_db = self._game_db.cursor()
        self.cursor_log_db = self._log_db.cursor()

   
    def game_data_check_and_delete(self, DB_NAME, TB_NAME, TIME_TYPE,TIME_DATA, DELETE_DATE):
        # TIME_TYPE :(1表示正常时间,2表示秒时间戳,3表示微秒时间戳)
        if int(TIME_TYPE) == 1:
            #query = "select {} from {}.{} where date({}) < '{}'".format(TIME_DATA,DB_NAME,TB_NAME,TIME_DATA,DELETE_DATE)
            query = "delete from {}.{} where date({}) < '{}';".format(DB_NAME,TB_NAME,TIME_DATA,DELETE_DATE)
        elif int(TIME_TYPE) == 2:
            #query = "select {} from {}.{} where date(FROM_UNIXTIME({})) < '{}'".format(TIME_DATA,DB_NAME,TB_NAME,TIME_DATA,DELETE_DATE)
            query = "delete from {}.{} where date(FROM_UNIXTIME({})) < '{}';".format(DB_NAME,TB_NAME,TIME_DATA,DELETE_DATE)
        elif int(TIME_TYPE) == 3:
            print("[time_type]: ", "3")
        else:
            print("wrong [time_type]")
        
        logging.info("查询语句: {}".format(query))
        print("查询语句: {}".format(query))
        self.get_db_connettions()
        self.cursor_game_db.execute(query)
        self._game_db.commit()
        time.sleep(1)

    def log_tb1_list(self, DB_NAME, TB_NAME, TIME_TYPE, DELETE_DATE, areaname):
        # TIME_TYPE: (1表示日期后缀, 2表示周一日期后缀, 3表示年月后缀, 4表示每月1号日期后缀, 5表示单下划线接日期后缀)
        # 遇到表名相同，后缀日期类型不同怎么办?使用正则表达式
        # 先列出表名，然后判断表名后缀的日期是否早于删除日期
        tb_file = 'table_list/{}/log_tb_list_{}.txt'.format(areaname, datetime.datetime.today().date())
        if Path(tb_file).exists():
            os.remove(tb_file)
        str1 = "[[:digit:]]{8}$" # 日期后缀，周表后缀，以及每月1号日期后缀
        str2 = "[[:digit:]]{6}$" # 年月后缀
        if int(TIME_TYPE) == 1 or int(TIME_TYPE) == 2 or int(TIME_TYPE) == 3 or int(TIME_TYPE) == 4 or int(TIME_TYPE) == 5:
                query = f"""SELECT TABLE_NAME FROM information_schema.`TABLES` where TABLE_SCHEMA = '{DB_NAME}' and CREATE_TIME < '{DELETE_DATE}' and (TABLE_NAME REGEXP '{TB_NAME}__{str1}' or TABLE_NAME REGEXP '{TB_NAME}__{str2}' or TABLE_NAME REGEXP '{TB_NAME}_{str1}')"""
                logging.info("查询语句: {}\n".format(query))
                #print("sql: ", query)
                self.get_db_connettions()
                self.cursor_log_db.execute(query)
                tb_list = self.cursor_log_db.fetchall()
                if tb_list:
                    # 先将可能要删除的日志表写入一个文件中
                    with open(tb_file, 'a+') as f:
                        for item in tb_list:
                            f.write(str(item[0])+'\n')
                    logging.info("{}前缀数量:{}".format(TB_NAME, len(tb_list)))
                else:
                    print("表不存在:{}".format(TB_NAME))
                    logging.info("表不存在:{}".format(TB_NAME))
        else:
            #print("其他类型")
            logging.info("其他类型")


    def log_tb2_list(self,DB_NAME, TB_NAME, TIME_TYPE,TIME_DATA, DELETE_DATE,areaname):
        DELETE_DATE = (datetime.datetime.today() + datetime.timedelta(days=-180)).strftime('%Y-%m-%d')
        DB_NAME = '{}_mj_game_log'.format(areaname)
        # TIME_TYPE :(1表示正常时间,2表示秒时间戳,3表示微秒时间戳)
        if int(TIME_TYPE) == 1:
            #query = "select {} from {}.{} where date({}) < '{}'".format(TIME_DATA,DB_NAME,TB_NAME,TIME_DATA,DELETE_DATE)
            query = "delete from {}.{} where date({}) < '{}'".format(DB_NAME,TB_NAME,TIME_DATA,DELETE_DATE)
        elif int(TIME_TYPE) == 2:
            #query = "select {} from {}.{} where date(FROM_UNIXTIME({})) < '{}'".format(TIME_DATA,DB_NAME,TB_NAME,TIME_DATA,DELETE_DATE)
            query = "delete from {}.{} where date(FROM_UNIXTIME({})) < '{}'".format(DB_NAME,TB_NAME,TIME_DATA,DELETE_DATE)
        elif int(TIME_TYPE) == 3:
            print("[time_type]: ", "3")
        else:
            print("wrong [time_type]")

        logging.info("sql语句: {}".format(query))
        #print("sql语句: {}".format(query))
        self.get_db_connettions()
        self.cursor_log_db.execute(query)
        self._log_db.commit()
        data = self.cursor_log_db.fetchall()

        logging.info("删除完成")
        

    def handle_game_data(self, areaname):
        DELETE_DATE = (datetime.datetime.today() + datetime.timedelta(days=-180)).strftime('%Y-%m-%d')
        DB_NAME = '{}_mj_game'.format(areaname)
        #print("处理市场:{}\n数据库:{}\n处理日期:{}".format(areaname,DB_NAME,DELETE_DATE))
        logging.info("\n##########处理市场:{}\n数据库:{}\n处理日期:{}\n###########".format(areaname,DB_NAME,DELETE_DATE))

        with open('game_tb/{}'.format(areaname), 'r+') as f:
            for line in f.readlines():
                data = list(line.split(','))
                TB_NAME,TIME_DATA,TIME_TYPE = data[0], data[1], data[2]
                #print("#########\n数据表:{}\n时间字段:{}\n时间类型:{}".format(TB_NAME,TIME_DATA,TIME_TYPE))
                logging.info("#########\n数据表:{}\n时间字段:{}\n时间类型:{}".format(TB_NAME,TIME_DATA,TIME_TYPE))
                self.game_data_check_and_delete(DB_NAME, TB_NAME, TIME_TYPE, TIME_DATA, DELETE_DATE)
                logging.info("###########{}.{}处理完成########".format(DB_NAME, TB_NAME))
        logging.info("{}{}业务数据处理完成\n".format(areaname,DELETE_DATE))
        time.sleep(2)

    def handle_log_data(self, areaname):
        DELETE_DATE = (datetime.datetime.today() + datetime.timedelta(days=-180)).strftime('%Y-%m-%d')
        DB_NAME = '{}_mj_game_log'.format(areaname)
        logging.info("\n############\n处理市场:{}\n数据库:{}\n处理日期:{}\n##########".format(areaname,DB_NAME,DELETE_DATE))
        # 正常情况下{}_1文件一定是存在的
        with open('log_tb/{}_1'.format(areaname), 'r+') as f:
            for line in f.readlines():
                data = list(line.split(','))
                TB_NAME,TIME_TYPE = data[0], data[1]
                #print("数据表:{}\n时间类型:{}".format(TB_NAME,TIME_TYPE))
                logging.info("数据表:{}\n时间类型:{}".format(TB_NAME,TIME_TYPE))
                self.log_tb1_list(DB_NAME, TB_NAME, TIME_TYPE,DELETE_DATE, areaname)
                self.delete_log_data(areaname)
                logging.info("###########{}.{}处理完成########".format(DB_NAME, TB_NAME))
        if Path('log_tb/{}_2'.format(areaname)).exists():
            with open('log_tb/{}_2'.format(areaname, 'r+')) as e:
                for line in e.readlines():
                    data = list(line.split(','))
                    TB_NAME,TIME_DATA,TIME_TYPE = data[0], data[1], data[2] 
                    #print("数据表:{}\n时间字段:{}\n时间类型:{}".format(TB_NAME,TIME_DATA,TIME_TYPE))
                    logging.info("数据表:{}\n时间字段:{}\n时间类型:{}".format(TB_NAME,TIME_DATA,TIME_TYPE))
                    self.log_tb2_list(DB_NAME,TB_NAME,TIME_TYPE,TIME_DATA, DELETE_DATE, areaname)
                    logging.info("###########{}.{}处理完成########".format(DB_NAME, TB_NAME))
        logging.info("{}{}日志数据处理完成\n\n\n".format(areaname,DELETE_DATE))
        time.sleep(2)        


    def delete_log_data(self, areaname):
        DB_NAME = '{}_mj_game_log'.format(areaname)
        tb_list = './table_list/{}/log_tb_list_{}.txt'.format(areaname, datetime.datetime.today().date())
        if Path(tb_list).exists():
            with open(tb_list, 'r') as e:
                for line in e.readlines():
                    sql  = "drop table {}.{}".format(DB_NAME, line)
                    logging.info("sql: {}".format(sql))
                            
                    self.get_db_connettions()
                    self.cursor_log_db.execute(sql)
                    logging.info("{}.{}数据删除完毕".format(DB_NAME, line))
                            
    def range_areaname(self):
        with open(self._areaname_list, 'r+') as f:
            for line in f.readlines():
                areaname = line.strip('\n')
                if Path('game_tb/{}'.format(areaname)).exists():
                    self.handle_game_data(areaname)
                    if Path('log_tb/{}_1'.format(areaname)).exists() or Path('log_tb/{}_2'.format(areaname)).exists():
                        self.handle_log_data(areaname)
                    else:
                        logging.warning("log tb file:[log_tb/{}_1 and log_tb/{}_2] is not exist".format(areaname, areaname))
                else:
                    logging.warning("game tb file:[game_tb/{}] is not exist".format(areaname))
                    if Path('log_tb/{}_1'.format(areaname)).exists() or Path('log_tb/{}_2'.format(areaname)).exists():
                        self.handle_log_data(areaname)
                    else:
                        logging.warning("log tb file:[log_tb/{}_1 and log_tb/{}_2] is not exist".format(areaname, areaname))
                        continue
        print(datetime.datetime.now())



test = DeleteData()
test.range_areaname()
logging.info("finished\n\n\n\n")
