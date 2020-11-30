#!/opt/anaconda3/bin/python3
import os
import datetime



def create_resource_file(file_path, day, month):
    profile_path = file_path + 'static_table.csv'
    tag_path = file_path + 'behavior_long_table.csv'
    profile_shell = """
    hive -e
    "SELECT
    a.brand AS db_brand,
    a.model AS db_model,
    b.age AS age,
    CASE b.sex WHEN 1 THEN '男' WHEN 0 THEN '女' ELSE '未知' END AS sex,
    a.province AS l_province,
    COUNT(*) AS uv
    FROM
    ( SELECT phone, province, brand, model FROM mobile.outdata_tmp
      WHERE d > {day} AND province IS NOT NULL AND province<>'' AND phone IS NOT NULL AND brand IS NOT NULL AND model IS NOT NULL ) a
    INNER JOIN 
    ( SELECT phone, age, sex FROM databank.user_info
      WHERE m = {month} AND age IS NOT NULL AND age>0 AND sex IS NOT NULL AND sex<>-1 GROUP BY phone, age, sex) b 
    ON a.phone = b.phone
    GROUP BY brand, model, age, sex, province;" > {profile_path}"""
    tag_shell = """
    hive -e
    "SELECT
    a.brand AS db_brand,
    a.model AS db_model,
    b.app AS app,
    COUNT(*) AS uv
    FROM
    ( SELECT phone, brand, model FROM mobile.outdata_tmp
      WHERE d > {day} AND brand IS NOT NULL AND model IS NOT NULL AND phone IS NOT NULL) a
    INNER JOIN 
    ( SELECT t2.phone AS phone, t1.appname AS app FROM pangxk_.app_local t1 INNER JOIN
      (SELECT phone, appid FROM databank.app_freq WHERE m = {month} AND appid IS NOT NULL GROUP BY phone, appid) t2
      ON t1.appid=t2.appid ) b 
    ON a.phone = b.phone
    GROUP BY brand, model, app;" > {tag_path}"""
    past_day = datetime.date.today() + datetime.timedelta(day * -1)
    past_day = int(past_day.strftime('%Y%m%d'))
    os.system(profile_shell.format(day=past_day, month=month, profile_path=profile_path))
    os.system(tag_shell.format(day=past_day, month=month, tag_path=tag_path))


def create_file_main(databank_profile_path, databank_tag_path, day, month):
    """"""
    past_day = datetime.date.today() + datetime.timedelta(day * -1)
    past_day = int(past_day.strftime('%Y%m%d'))
    create_table()
    insert_data_to_table(past_day, month)
    create_file(databank_profile_path, databank_tag_path)


def create_table():
    """ 建表 """
    create_table_shell = """hive -e "use car_stat;
    drop table if exists static_user_info;
    create table if not exists static_user_info
        (
        phone string,
        age int,
        sex int
        )
    row format delimited
    fields terminated by ','
    stored as orc;
    drop table if exists static_outdata_tmp;
    create table if not exists static_outdata_tmp
        (
        phone string,
        province string,
        brand string,
        model string
        )
    row format delimited
    fields terminated by ','
    stored as orc;
    drop table if exists static_join_table;
    create table if not exists static_join_table
        (
        db_brand string,
        db_model string,
        age int,
        sex int,
        l_province string
        )
    row format delimited
    fields terminated by ','
    stored as orc;
    drop table if exists static_table;
    create table if not exists static_table
        (
        db_brand string,
        db_model string,
        age int,
        sex string,
        l_province string,
        uv int
        )
    row format delimited
    fields terminated by ','
    stored as orc;
    drop table if exists behavior_join_table;
    create table if not exists behavior_join_table
        (
        db_brand string,
        db_model string,
        appid string
        )
    row format delimited
    fields terminated by ','
    stored as orc;
    drop table if exists behavior_join_table_appname;
    create table if not exists behavior_join_table_appname
        (
        db_brand string,
        db_model string,
        app string
        )
    row format delimited
    fields terminated by ','
    stored as orc;
    drop table if exists behavior_long_table;
    create table if not exists behavior_long_table
        (
        db_brand string,
        db_model string,
        app string,
        uv int
        )
    row format delimited
    fields terminated by ','
    stored as orc;"
    """
    os.system(create_table_shell)


def insert_data_to_table(past_day, month):
    """ 往hive表中插入数据 """
    insert_data_to_table_shell = """hive -e "use car_stat;
    insert into static_user_info
        SELECT phone, age, sex 
        FROM databank.user_info
        WHERE op='liantong' AND m = {month}
            AND age IS NOT NULL 
            AND age>0 
            AND sex IS NOT NULL
            AND sex<>-1 
        GROUP BY phone, age, sex;
    insert into static_outdata_tmp
        SELECT phone, province, brand, model
        FROM mobile.outdata_tmp
        WHERE d > {past_day}
            AND province IS NOT NULL
            AND province<>''
            AND phone IS NOT NULL
            AND brand IS NOT NULL
            AND model IS NOT NULL
        GROUP BY phone, province, brand, model;
    insert into static_join_table
        SELECT a.brand, a.model, b.age, b.sex, a.province
        FROM car_stat.static_outdata_tmp a 
            INNER JOIN car_stat.static_user_info b
            ON a.phone = b.phone;
    insert into static_table
        SELECT
            db_brand,
            db_model,
            age,
            CASE sex WHEN 1 THEN '女' WHEN 0 THEN '男' ELSE '未知' END AS sex,
            l_province,
            COUNT(*) AS uv
        FROM 
            car_stat.static_join_table
        GROUP BY db_brand, db_model, age, sex, l_province;
    insert into behavior_join_table
        SELECT a.brand, a.model, b.appid
        FROM car_stat.static_outdata_tmp a
        INNER JOIN
            (SELECT phone, appid FROM databank.app_freq
             WHERE op='liantong' AND m = {month} AND appid IS NOT NULL) b
        ON a.phone=b.phone;
    insert into behavior_join_table_appname
        SELECT b.db_brand, b.db_model, a.appname
        FROM car_stat.appinfo a
        INNER JOIN car_stat.behavior_join_table b
        ON a.appid=b.appid;
    insert into behavior_long_table
        SELECT db_brand, db_model, app, COUNT(*)
        FROM car_stat.behavior_join_table_appname
        GROUP BY db_brand, db_model, app;"
    """
    os.system(insert_data_to_table_shell.format(past_day=past_day, month=month))


def create_file(static_table_file, behavior_long_table_file):
    """ 读取hive表中的数据保存为文件 """
    create_file_shell1 = """hive -e "use car_stat;
    set hive.cli.print.header=false;
    select db_brand, db_model, age, sex, l_province, uv from car_stat.static_table;" > {static_table_file}"""
    create_file_shell2 = """hive -e "use car_stat;
    set hive.cli.print.header=false;
    select db_brand, db_model, app, uv from car_stat.behavior_long_table;" > {behavior_long_table_file}"""
    os.system(create_file_shell1.format(static_table_file=static_table_file))
    os.system(create_file_shell2.format(behavior_long_table_file=behavior_long_table_file))


if __name__ == '__main__':
    databank_profile_path = "/home/wangl/work/data/databank_sta/static_table.csv"
    databank_tag_path = "/home/wangl/work/data/databank_sta/behavior_long_table.csv"

    # 生成文件
    # day = 30  # 使用近30的数据
    # month = 202008
    # create_file_main(databank_profile_path, databank_tag_path, day, month)
    import pandas as pd
    import csv
    static_table = pd.read_csv(databank_profile_path, header=None, sep='\t')
    print(len(static_table))
    print(list(static_table))
    print(static_table.head())
    print('*'*100)
    # behavior_long_table = pd.read_csv(databank_tag_path, error_bad_lines=False, header=None, sep='\t', quoting=csv.QUOTE_NONE)
    behavior_long_table = pd.read_csv(databank_tag_path, header=None, sep='\t', quoting=csv.QUOTE_NONE)
    print(len(behavior_long_table))
    print(list(behavior_long_table))
    print(behavior_long_table.head())
