#!/usr/bin/python
# _*_coding:utf-8_*_
__author__ = 'wencycool'
__date__ = '2017-02-25'
__version__ = 1.9
"""
涉及思路以及主要功能点：
方便于大批量SQL优化，代替传统手工方式，极大提高生产率
1.采用get snapshot 方式获取性能信息而非mon_get视图，因为snapshot可以分析一段时间内的SQL执行信息
2.可自动将等值、非等值、in、between and等相似条件的SQL语句合并成一种类型，自动解析SQL中表名得出schema；
3.按照执行次数，执行时间，读取记录数等条件筛选出需要分析的SQL语句；
4.自动合并索引，自动分析出受益SQL
5.打印html报告包括TOPSQL信息，索引信息，索引上表信息，表上已有索引信息，SQL详细信息等
#权限要求：需要具有get snapshot 权限、db2advis权限、查看系统视图的权限
v1.3
该版本为可用版本第一版
该版本将snapshot的输出直接通过管道输入到本程序中，不进行落地
该程序主要将db2的dynamic sql进行格式化解析到sqlite数据库中，并且对符合条件的sql做advis分析
利用函数将advis进行格式化输入最后产出报表
v1.4
新增-f 选项，可以只对一段时间内的动态sql进行分析,即支持离线情况下的动态sql分析
v1.5
1.因aix上无法安装sqlite,将sqlite数据库存储方式改写为数组方式存储，并优化内存使用空间
2.优化数组遍历改为字典遍历，性能大幅提高
3.增加cpu时间消耗，物理度，逻辑读等维度以方便更直观的分析
4.增强html报表输出，让输出更加美观
v1.6
支持增加TOPsql的排序功能即按照平均执行时间降序方式做advis分析,注意界面中并未排序
v1.7
修复advis_format函数中的BUG,之前打印的受益SQL列表有问题,经过修改后可以正确打印
v1.8
增加-t选项,可以直接利用-t得到某些分钟之内的快照信息,等同于db2 reset monitor all ;sleep N*60;db2 get snapshot for dynamic on <dbname>
修改html的charset=GB2312 代替原来的utf-8保证中文可以正常打印
v1.9
20171101 利用sql_text来代替sql_text_format保留原生SQL中的一条，这样可以更准确的评估执行计划，更换代码如下：
sql_md5_ref[sql_text_format_md5] = sql_text_format
sql_md5_ref[sql_text_format_md5] = sql_text #sql_text_format
#？考虑是否需要将SQL文本全部改成小写?正：1.有的sql在忽略大小写情况下文本一致；反：2.在双引号参与下表名确实存在大小写
"""
import sys

stdout = sys.stdout
reload(sys)
sys.setdefaultencoding('utf8')
sys.stdout = stdout
import hashlib
import subprocess
import re
import os
import time
from optparse import OptionParser
import logging
import socket
import getpass


def get_db2_tabschema(dbname):
    tabname_dict = {}
    cmd = ' db2 connect to ' + dbname + '>/dev/null;db2 -x "select tabschema,tabname from syscat.tables where type=\'T\' and tabschema not like \'SYS%\' with ur";echo "";'
    # stdout,stderr,returncode = command_run(cmd)
    proc = subprocess.Popen(cmd, bufsize=100, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = proc.communicate()
    for i in stdout.split('\n'):
        myList = re.split('\s+', i.strip())
        if len(myList) == 2: tabname_dict[myList[1]] = myList[0]
        tabname_dict['None'] = 'None'
    return tabname_dict


def get_tabschema(tabname, tabname_dict):
    tabname = tabname.upper()
    if tabname_dict.has_key(tabname):
        return tabname_dict[tabname].upper()
    else:
        return 'None'


def get_tabname(sql_text):
    # pattern_text='from\s+?("?(?P<first>\w+?)\s*?"?\.)?"?(?P<last>\w+) *"?'
    pattern_text = '(from|delete\s+from|update)\s+("?\w+"?\.)?"?(?P<last>\w+)"?'
    pattern_tab = re.compile(pattern_text, re.I)
    m = pattern_tab.search(sql_text)
    if m is not None and m.group('last') is not None:
        return m.group('last').upper()
    else:
        return "None"


def get_tabnameALL(sql_text):
    tablist = []
    # pattern_text='from\s+?("?(?P<first>\w+?)\s*?"?\.)?"?(?P<last>\w+) *"?'
    pattern_text = '(from|delete\s+from|update)\s+("?\w+"?\.)?"?(?P<last>\w+)"?'
    while len(sql_text) > 0:
        pattern_tab = re.search(pattern_text, sql_text, re.I)
        if pattern_tab is not None:
            tablist.append(pattern_tab.group("last"))
            sql_text = sql_text[pattern_tab.end():]
        else:
            return tablist


def getNameFromIdx(create_index_text):
    '''从标准索引创建语句中解析出表模式名和表名'''
    pattern_text = 'on\s+(?P<tabschema>\w+)\s{0,}\.(?P<tabname>\w+)'
    pattern_tab = re.compile(pattern_text, re.I)
    m = pattern_tab.search(create_index_text)
    if m is not None and m.group("tabschema") is not None and m.group("tabname") is not None:
        return (m.group("tabschema"), m.group("tabname"))
    else:
        return ('', '')


# 设置与shell交互窗口,默认超时时间为2分钟
def command_run(command, timeout=30):
    proc = subprocess.Popen(command, bufsize=0, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    poll_seconds = .250
    deadline = time.time() + timeout
    while time.time() < deadline and proc.poll() == None:
        time.sleep(poll_seconds)
    if proc.poll() == None:
        if float(sys.version[:3]) >= 2.6:
            proc.terminate()
    stdout, stderr = proc.communicate()
    return stdout, stderr, proc.returncode


# 多线程调用command_run,加速advis（注意如果设置过大会导致主机资源使用率较高一般设置5个以下）
# def mutiCommand_run(command,mutiNum)

def getHashvalue(sql_text):
    m = hashlib.md5()
    m.update(sql_text)
    result = m.hexdigest()
    del m
    return result


# 矩阵运算函数
def compList(flag="+", *myList):
    myList_len = len(myList)
    if myList_len < 2: return False
    gen_len = len(myList[0])
    for oneList in myList:
        if gen_len <> len(oneList): return False
    resList = []
    if flag == "+":
        for i in range(gen_len):
            sum_sem = 0
            for j in range(myList_len):
                sum_sem = myList[j][i] + sum_sem
            resList.append(sum_sem)
    elif flag == "-":
        for i in range(gen_len):
            minus_sem = myList[0][i]
            for j in range(1, myList_len):
                minus_sem = minus_sem - myList[j][i]
            resList.append(minus_sem)
    elif flag == "*":
        for i in range(gen_len):
            multi_sem = 1
            for j in range(myList_len):
                multi_sem = myList[j][i] * multi_sem
            resList.append(multi_sem)
    elif flag == "/":
        for i in range(gen_len):
            dev_sem = myList[0][i]
            for j in range(1, myList_len):
                dev_sem = round(float(dev_sem) / myList[j][i], 2)
            resList.append(dev_sem)
    else:
        return False
    return resList


# 将SQL做归类处理
def sqlFormat(sql_text):
    # 处理等于或者不等于情况
    p = re.compile(r'(?P<first>=|<>|<|>|!=)\s*?(\d+|\'[\s\S]*?\')\s*', re.I)
    p_replace = r'\g<first> ? '
    # 将多个连续空白字符缩短成一个空格
    p_blank = re.compile(r'\s{1,}', re.I)
    p_blank_replace = r' '
    # p.sub(r'\g<first> ? ' ,str1)
    # 处理in的情况
    p1 = re.compile(r'\bin\b\s*\((?!\s*?select)[\s\S]*?\)', re.I)
    p1_replace = r' in ( ? ) '
    # p1.sub(r' in ( ? ) ' ,str1)
    # 处理between and情况
    p2 = re.compile(r'\bbetween\b\s*?(\?|\d+|\'[\w\W]*?\')\s*?\band\b\s*?(\?|\d+|\'[\w\W]*?\')', re.I)
    p2_replace = r'between ? and ? '
    sql_format = p.sub(p_replace, sql_text)
    sql_format = p1.sub(p1_replace, sql_format)
    sql_format = p2.sub(p2_replace, sql_format)
    sql_format = p_blank.sub(p_blank_replace, sql_format)
    sql_format_md5 = getHashvalue(sql_format)
    return (sql_format_md5, sql_format)


# '
# 索引解析函数
def advis_format(fileName, recommend=90, maxColCount=4, recMerge=True, recMergeMin=1, mutiMerge=True):
    """
    :param fileName:
    :param recommend: 为索引推荐值，当索引推荐度不小于此值时会打印出该索引，取值范围[0:100]
    :param maxColCount: 为保留的索引包含字段的最大个数,默认情况保留4个字段
    :param recMerge: 为是否递归向前合并索引,即如果同时存在idx1(a,b,c)和idx2(a,b)和idx3(a)会合并成为idx1(a,b,c)
    :param recMergeMin: 即合并字段的最小值,在recMerge为True的情况下当索引字段个数超过recMergeMin的情况下才会发生索引合并,默认值为1：即全部进行前序子集合并
                        如果改值为2时，即当索引中字段个数少于2的时候该索引不会参与合并处理，例如同时存在idx1(a,b,c)和idx2(a,b)和idx3(a)会合并成为idx1(a,b,c);idx3(a)
    :param mutiMerge: 为当索引中字段个数相同的情况下如果改值为True那么会忽略字段顺序，进行索引合并；如果为False则不进行合并。
    :return: 输出结果为索引详细信息以及SQLMD5链和无法advis效率低下advis SQLMD5链
    """
    # file_name=r'dyn.txt.out'
    file_name = fileName
    myList = []
    myDict = {}
    sql_format_md5_list = []
    # 标记字典中可删除的元素
    del_keys_list = []
    idx_size = 0  # MB
    improvement_percent = 0
    # 定义不足够做索引推荐的sqlmd5存储列表
    lower_recommend_sql_format_md5_list = []
    p0 = re.compile("""^[\d\w]+$""")  # sqlmd5
    p1 = re.compile("""total disk space needed for initial set \[\s*(?P<first>\d+\.\d+)\] MB""")
    p2 = re.compile("""total disk space constrained to         \[\s*(?P<first>\d+\.\d+)\] MB""")
    p3 = re.compile(""" \[\s*(?P<first>\d+\.\d+)\] timerons  \(without recommendations\)""")
    p4 = re.compile("""\[\s*(?P<first>\d+\.\d+)\] timerons  \(with current solution\)""")
    p5 = re.compile("""\[\s*(?P<first>[-]?\d+\.\d+)%\] improvement""", re.I)
    p_idx_begin = re.compile("""-- index\[\d{1,2}\],\s+(?P<first>\d+\.\d+)MB""")
    p_idx_end = re.compile("""   COMMIT WORK ;""")
    index_unit_flag = 0
    indexStr = ""
    sql_format_md5 = ""
    recommend_sql_format_md5 = ""
    if maxColCount < 1: maxColCount = 1
    try:
        with open(file_name) as f:
            line = f.readline()
            while line:
                m = p0.search(line)
                if m is not None:
                    sql_format_md5 = m.group()
                    # print sql_format_md5
                # total disk space needed for initial set
                m = p1.search(line)
                if m is not None: disk_space_initial = m.group("first")
                # total disk space constrained to
                m = p2.search(line)
                if m is not None: disk_space_constrained = m.group("first")
                # improvement
                m = p5.search(line)
                if m is not None:
                    improvement_percent = m.group("first")
                    if float(improvement_percent) < float(recommend):
                        if sql_format_md5 not in lower_recommend_sql_format_md5_list: lower_recommend_sql_format_md5_list.append(
                            [sql_format_md5])
                    else:
                        recommend_sql_format_md5 = sql_format_md5
                if index_unit_flag == 1:
                    indexStr = indexStr + line.strip() + " "
                m = p_idx_begin.search(line)
                if m is not None:
                    index_unit_flag = 1
                    idx_size = m.group("first")
                m = p_idx_end.search(line)
                if m is not None:
                    index_unit_flag = 0
                    # 去掉双引号
                    indexStr = re.sub(r'"', "", indexStr)
                    # 将连续多个空白符替换成一个空格
                    indexStr = re.sub(r'\s+', " ", indexStr)
                    # 去掉点前面的空格
                    indexStr = re.sub('\s+\.', ".", indexStr)
                    # 将非分区索引修改为分区索引
                    indexStr.replace('NOT PARTITIONED', 'PARTITIONED')
                    # 处理索引中的字段将字段缩减到指定的范伟maxColCount之内
                    col_format_str = r'\((?P<first>\w+\s+(ASC|DESC))(?P<second>(,\s+\w+\s(ASC|DESC)){0,' + str(
                        maxColCount - 1) + '})(,\s+\w+\s(ASC|DESC))*\)'
                    indexStr = re.sub(col_format_str, r'(\g<first>\g<second>)', indexStr, re.I)
                    key = indexStr.split(' ON ', 1)[1]
                    exists_flag = 0
                    if float(improvement_percent) >= float(recommend) and indexStr.find('CREATE UNIQUE INDEX') == -1:
                        for myKey in myDict:
                            # myDict[myKey][0]为标志位，如果为0则代表该记录无效，不能用于循环查找；如果为1代表有效
                            if myDict[myKey][0] == 0: continue
                            myKey_p = re.search(col_format_str, myKey)
                            key_p = re.search(col_format_str, key)
                            myKeyDiff = re.sub(r'ASC|DESC', r'ASC', myKey_p.group())[1:-1].strip()
                            keyDiff = re.sub(r'ASC|DESC', r'ASC', key_p.group())[1:-1].strip()
                            # 去重
                            if re.sub(r'ASC|DESC', r'ASC', myKey) == re.sub(r'ASC|DESC', r'ASC', key):
                                exists_flag = 1
                                myDict[myKey][1][4] = list(set(myDict[myKey][1][4] + [recommend_sql_format_md5]))
                                break
                            # 忽略字段顺序去重
                            if mutiMerge == True:
                                myKeyDiffToList = list(myKeyDiff)
                                keyDiffToList = list(keyDiff)
                                myKeyDiffToList.sort()
                                keyDiffToList.sort()
                                myKeyDiffSorted = "".join(myKeyDiffToList)
                                keyDiffsorted = "".join(keyDiffToList)
                                if myKeyDiffSorted == keyDiffsorted:
                                    exists_flag = 1
                                    # 去重复后保留sqlMD5连接
                                    myDict[myKey][1][4] = list(set(myDict[myKey][1][4] + [recommend_sql_format_md5]))
                                    break
                            # 递归合并
                            if recMerge == True:
                                if myKey_p is not None and key_p is not None:
                                    if len(myKeyDiff.split(',')) < recMergeMin or len(keyDiff.split(',')) < recMergeMin:
                                        pass
                                    # 索引开始位置相同
                                    elif myKeyDiff.find(keyDiff) == 0:
                                        # 1为已经存在
                                        exists_flag = 1
                                        # 去重复后保留sqlMD5连接
                                        myDict[myKey][1][4] = list(
                                            set(myDict[myKey][1][4] + [recommend_sql_format_md5]))
                                        break
                                    # 当删除一个已知索引后保留被删除索引的sqlMD5连接
                                    elif keyDiff.find(myKeyDiff) == 0:
                                        sql_format_md5_list = list(
                                            set(myDict[myKey][1][4] + [recommend_sql_format_md5]))
                                        myDict[myKey][0] = 0
                                        exists_flag = 2
                                        break
                        if exists_flag == 0:
                            # 如果是新加入的索引则直接添加
                            sql_format_md5_list = [recommend_sql_format_md5]
                            value = [1, [indexStr.split(' ON ', 1)[0], disk_space_initial, disk_space_constrained,
                                         improvement_percent, sql_format_md5_list, idx_size]]
                            myDict[key] = value
                        elif exists_flag == 2:
                            value = [1, [indexStr.split(' ON ', 1)[0], disk_space_initial, disk_space_constrained,
                                         improvement_percent, sql_format_md5_list, idx_size]]
                            myDict[key] = value
                        sql_format_md5_list = []
                    indexStr = ""
                line = f.readline()
        for key, value in myDict.items():
            value = value[1]
            if myDict[key][0] == 0: continue
            # myList.append([value[0] + ' ON ' + key] + value[1:4] + [" , ".join(value[4])] + [value[5]])
            # Improvement,Index Size,Create Index Text,SQL MD5 Chain
            myList.append([value[3]] + [value[5]] + [value[0] + ' ON ' + key] + [" , ".join(value[4])])
        return (myList, lower_recommend_sql_format_md5_list)
    except IOError as e:
        print e


###############定义HTML报表函数#################
def html_head():
    str = """
    <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN""http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
    <html xmlns="http://www.w3.org/1999/xhtml">
    <head>
    <meta http-equiv="Content-Type" content="text/html; charset=GB2312" />
    <title>DB2 Snapshot Information</title>
    <link href="styles.css" rel="stylesheet" type="text/css" />
    </head>
    <style type="text/css">
    table.altrowstable {
        font-family: verdana,arial,sans-serif;
        font-size:11px;
        color:#333333;
        border-width: 1px;
        border-color: #a9c6c9;
        border-collapse: collapse;
    }
    table.altrowstable th {
        border-width: 1px;
        padding: 8px;
        border-style: solid;
        border-color: #a9c6c9;
    }
    table.altrowstable td {
        border-width: 1px;
        padding: 8px;
        border-style: solid;
        border-color: #a9c6c9;
    }
    .oddrowcolor{
        background-color:#d4e3e5;
    }
    .evenrowcolor{
        background-color:#c3dde0;
    }
    </style>
    """
    str = str + '\n'
    return str


# 标题定义函数，支持定义标题大小和位置
def html_title(str, title_pos='center', title_level='h1', blanks=0, name=''):
    html_str = '<br>' * blanks
    title_level = title_level.lower()
    title_pos = title_pos.lower()
    title_level_list = ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']
    title_pos_list = ['left', 'center', 'right', 'justify']
    if title_level not in title_level_list: title_level = 'h1'
    if title_pos not in title_pos_list: title_pos = 'left'
    html_str = html_str + '<p><a name="' + name + '"></a></p>' + '\n'
    html_str = html_str + '<' + title_level + ' align="' + title_pos + '">' + str + '</' + title_level + '> \n'
    return html_str


def html_body(content):
    str_list = []
    str_list.append(r'<body>')
    str_list.append(content)
    str_list.append(r'</body>')
    return "\n".join(str_list)


def html_table(caption, th_list, data_list, intervalflag=1):
    '''caption 标题名称，如果为空则不显示标题
    th_list是一维数组，是表格中的主题元素；
    data_list是二维数组,生成表格中数据
    intervalflag 值为0的时候不会自动间隔着色，其它值则会间隔着色，默认为1即间隔着色
    '''
    str_list = []
    str_list.append(r'<table border="1" class="altrowstable" id="alternatecolor">')
    if len(caption) <> 0: str_list.append(
        r'<caption style="font-family:arial;font-size:20px">' + caption + r'</caption>')
    # str_list.append(r'<colgroup span="1" width="200"></colgroup>')
    # str_list.append(r'<colgroup span="3" width="400"></colgroup>')
    # str_list.append(r'<thead>')
    # if len(th_list) == len(data_list):
    str_list.append(
        r'<tr style="color: black; background-color: #80b8d0; font-weight: bold; font-size: 10pt" valign="top">')
    str_list.append(r'<th>Number</th>')
    for row in th_list:
        str_list.append(r'<th>')
        str_list.append(str(row))
        str_list.append(r'</th>')
    str_list.append(r'</tr>')
    lineno = 0
    nbr = 0
    if intervalflag == 0:
        for row in data_list:
            nbr = nbr + 1
            str_list.append(r'<tr>')
            str_list.append(r'<td>' + str(nbr) + '</td>')
            for column in row:
                str_list.append(r'<td>' + str(column) + r'</td>')
            str_list.append(r'</tr>')
            lineno = lineno + 1
    else:
        for row in data_list:
            flag = 0
            nbr = nbr + 1
            if lineno % 2 == 1: flag = 1
            if flag == 1:
                str_list.append(r'<tr bgcolor="#cfcfcf">')
            else:
                str_list.append(r'<tr bgcolor="white">')
            str_list.append(r'<td>' + str(nbr) + '</td>')
            for column in row:
                str_list.append(r'<td>' + str(column) + r'</td>')
            str_list.append(r'</tr>')
            lineno = lineno + 1
    str_list.append(r'</table>')
    return "\n".join(str_list) + '<br>'


###############定义HTML报表函数#################
##################FUNCTION##################
if __name__ == '__main__':
    # 配置日志打印
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S')
    proj_name = os.path.realpath(sys.argv[0])
    ######自定义参数区间-start######
    # 筛选结果为:执行次数 and (平均记录数> xx or 平均执行时间>mm)
    executions_ctrl = 30  # 默认为50个
    avg_time_ctrl = 0.5  # 默认为1秒
    avg_read_ctrl = 50000  # 默认为100000
    original_snap_rows_ctrl = 100000  # 控制所有需要处理的SQL语句的最大个数，默认值为10w
    format_snap_rows_ctrl = 10000  # 控制结构化后可被存储在字典中的SQL信息的最大个数，默认为10000；
    advis_ctrl = 500  # 控制需要advis的SQL的个数
    sql_text_length_ctrl = 10000  # 控制SQL语句的最大字符个数，超过改值则该SQL不进行解析;默认为1w
    out_file = 'advis.out'
    ######自定义参数区间-end######
    original_snap_rows_ctrl_flag = 0  # 原始SQL溢出标记
    format_snap_rows_ctrl_flag = 0  # 格式化后SQL溢出标记
    usage = "python " + proj_name + " -d <dbname> [[-f <filename>]|[-t <min>]]"
    parser = OptionParser(usage)
    parser.add_option("-d", "--database", action="store", type="string", dest="dbname", help="database name")
    parser.add_option("-f", "--filename", action="store", type="string", dest="dynfile", help="dynamic sql file name")
    parser.add_option("-t", "--time", action="store", type="string", dest="difftime", help="sleep time [minutes]")
    options, args = parser.parse_args()
    if options.dbname is not None:
        dbname = options.dbname.lower()
    else:
        print "No database list!"
        print parser.print_help()
        exit()
    if options.dynfile is not None:
        cmd = 'cat ' + options.dynfile
    elif options.difftime is not None and str(options.difftime).isdigit():
        sleeptime = int(options.difftime) * 60
        # 在aix平台上Python 2.7.5在reset monitor 之后db2 get snapshot for dynamic sql on <dbname> 还是全量抽取,必须在后面加上awk 进行过滤一下就没问题，在Linux上也无此问题
        cmd = "db2 reset monitor all;sleep %s;db2 get snapshot for dynamic sql on %s|awk '{print}'" % (
        sleeptime, dbname)
    else:
        cmd = 'db2 get snapshot for dynamic sql on ' + dbname
    hostname = socket.gethostname()
    user = getpass.getuser()
    curtime = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    html_outputfile = "__".join([hostname, user, dbname, curtime, "SQLPerformanceAnalysis.html"])
    snapRow = []  # 将单元快照信息整合成行
    snapRowsDict = {}  # 存放整合后的snap快照信息
    sql_md5_ref = {}  # 存放md5 与sql的关系
    resultDict = {}  # 存放最终需要advis的快照信息
    normDict = {}  # 存放可以advis的快照信息
    errDict = {}  # 存放无法advis的快照信息
    sql_text = ''
    flag = 0
    dyn_flag = 0
    fw = open(out_file, 'w')
    logging.info("Get snapshot ...")
    # cmd = 'db2 get snapshot for dynamic sql  on ' + dbname
    logging.info("get snapshot command:" + cmd)
    # 解析dynamic sql
    p_snap = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    num = 0
    try:
        while 1:
            line = p_snap.stdout.next()
            line_format = [s.strip() for s in line.split('=', 1)]
            if line.find('Dynamic SQL Snapshot Result') > -1: dyn_flag = 1
            if line.find('Application Snapshot') > -1: break
            if dyn_flag == 1:
                # if line.find('Database name') > -1:dbname = line_format[1]
                if line.find('Number of executions') > -1:
                    flag = 1
                    if len(sql_text) > sql_text_length_ctrl:
                        sql_text = "SQL Text is too long"
                    sql_text_format_md5 = sqlFormat(sql_text)[0]
                    sql_text_format = sqlFormat(sql_text)[1]
                    snapRow = map(float, snapRow)
                    if len(snapRow) == 29 and int(snapRow[0]) > 0:
                        num = num + 1
                        if num >= original_snap_rows_ctrl:
                            original_snap_rows_ctrl_flag = 1
                            break
                        if len(snapRowsDict) >= format_snap_rows_ctrl:
                            format_snap_rows_ctrl_flag = 1
                            break
                        # 20171101 利用sql_text来代替sql_text_format保留原生SQL中的一条，这样可以更准确的评估执行计划
                        sql_md5_ref[sql_text_format_md5] = sql_text  # sql_text_format
                        # 字典时间复杂度O(1),数组为O(n)
                        if sql_text_format_md5 in snapRowsDict:
                            snapRowsDict[sql_text_format_md5] = compList('+', snapRowsDict[sql_text_format_md5],
                                                                         snapRow)
                        else:
                            snapRowsDict[sql_text_format_md5] = snapRow
                    snapRow = []
                    sql_text = ''
                if line.find('Statement text') > -1:
                    flag = 2
                    sql_text = line_format[1]
                    continue
                if flag == 1: snapRow.append(line_format[1])
                if flag == 2:
                    if len(line.strip()) == 0: continue
                    sql_text = sql_text + ' ' + line.strip()
    except StopIteration as e:
        pass
    # logging.info("Analysis Snapshot Used Memory size:" + str((sys.getsizeof(snapRowsDict)+sys.getsizeof(sql_md5_ref))/1024/1024) + "MB")
    if original_snap_rows_ctrl_flag == 1: logging.info("Rows exceed original snap rows ctrl !")
    logging.info("Total original SQLs count:" + str(num))
    if format_snap_rows_ctrl_flag == 1: logging.info("Rows exceed format snap rows ctrl !")
    logging.info("Total formated SQLs count:" + str(len(snapRowsDict)))
    logging.info("Get snapshot complete !")
    logging.info("Get db2 tabschema list ...")
    tabname_dict = get_db2_tabschema(dbname)
    logging.info("Start advis sql(may take a long time) ...")
    for key, value in snapRowsDict.items():
        executions = int(value[0])
        total_rows_read = int(value[6])
        total_exec_time = round(value[24], 2)
        total_cpu_time = round(value[25] + value[26], 2)
        avg_rows_read = int(value[6] / value[0])
        avg_exec_time = round(value[24] / value[0], 2)
        avg_lgcl_reads = int(value[12] / value[0])
        avg_pycl_reads = int(value[13] / value[0])
        avg_idx_lgcl_reads = int(value[16] / value[0])
        avg_idx_pycl_reads = int(value[17] / value[0])
        avg_cpu_time = round((value[25] + value[26]) / value[0], 2)
        sql_text = sql_md5_ref[key]
        tabschema = get_tabschema(get_tabname(sql_text), tabname_dict)
        if tabschema != 'None' and executions > executions_ctrl and (
                avg_rows_read > avg_read_ctrl or avg_exec_time > avg_time_ctrl):
            resultDict[key] = [executions, total_rows_read, total_exec_time, total_cpu_time, avg_rows_read,
                               avg_exec_time, avg_cpu_time, avg_lgcl_reads, avg_pycl_reads, avg_idx_lgcl_reads,
                               avg_idx_pycl_reads]
        del snapRowsDict[key]
    del snapRowsDict
    advis_count = len(resultDict)
    if advis_count > advis_ctrl:
        logging.info("Advis SQL count :" + str(advis_count) + ",But only " + str(
            advis_ctrl) + " SQL will be give asdvise suggest!")
        advis_count = advis_ctrl
    else:
        logging.info("Advis SQL count :" + str(advis_count))
    num = 0
    # 构造数组对可疑做advis的语句按照指定列进行排序，默认按照平均执行时间
    sorted_resultDict_tolist = []
    for key, value in resultDict.items(): sorted_resultDict_tolist.append(value + [key])
    # 对sorted_resultDict_tolist二位数组按照平均执行时间即数组中第6个元素进行排序
    sorted_resultDict_tolist.sort(key=lambda x: x[6], reverse=True)
    for value in sorted_resultDict_tolist:
        key = value[-1]
        num = num + 1
        if num > advis_ctrl: break
        # sql_format_md5 = key
        sql_text = sql_md5_ref[key]
        tabschema = get_tabschema(get_tabname(sql_text), tabname_dict)
        if len(sql_text) > 100:
            sql_text_short = sql_text[:100] + '...'
        else:
            sql_text_short = sql_text
        cmd = 'db2advis -d ' + dbname + ' -s "' + sql_text + '" -q ' + tabschema + ' -n ' + tabschema
        stdout, stderr, returncode = command_run(cmd)
        if returncode == 0:
            normDict[key] = [tabschema, sql_text_short, key, 'YES']
            fw.write(key + '\n')
            fw.write(stdout + '\n')
        else:
            errDict[key] = [tabschema, sql_text_short, key, 'BAD']
        sys.stdout.write('{2}{0}/{1}\r'.format(num, str(advis_count), "Progress:"))
        sys.stdout.flush()
    del sorted_resultDict_tolist
    goodAdvisList, badAdvisList = advis_format(out_file)
    for myList in badAdvisList:
        if myList[0] in normDict:
            normDict[myList[0]] = normDict[myList[0]][:-1] + ['NO']
    logging.info("Advis sql analysis sucessfully !")
    # 从DB2数据库中获取syscat.tables,syscat.indexes表结构
    logging.info("Start extract table and index information from db2 database ...")
    getTab_cmd = 'db2 connect to ' + dbname + ' >/dev/null ;db2 -x "select ltrim(rtrim(tabschema)) as tabschema,ltrim(rtrim(tabname)) as tabname,type,(select count(*) from syscat.indexes b where b.tabschema=a.tabschema and b.tabname=a.tabname) as idx_cnt,card,stats_time,create_time  from syscat.tables a where type in (\'T\',\'V\') and tabschema not like \'SYS%\' with ur";echo "" '
    getIdx_cmd = 'db2 connect to ' + dbname + ' >/dev/null ;db2 -x "select ltrim(rtrim(indschema)) as indschema,ltrim(rtrim(indname)) as indname,ltrim(rtrim(tabschema)) as tabschema,ltrim(rtrim(tabname)) as tabname,create_time,colnames from syscat.indexes where tabschema not like \'SYS%\' with ur";echo "" '
    proc = subprocess.Popen(getTab_cmd, bufsize=0, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = proc.communicate()
    db2_tab_list = [re.split('\s+', row)[:7] for row in stdout.split('\n') if len(re.split('\s+', row)) >= 7]
    proc = subprocess.Popen(getIdx_cmd, bufsize=0, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = proc.communicate()
    db2_idx_list = [re.split('\s+', row)[:6] for row in stdout.split('\n') if len(re.split('\s+', row)) >= 6]
    logging.info("Get table and index information from db2 database sucessfully !")
    logging.info("Start generate report ...")
    for key in normDict: normDict[key] = resultDict[key] + normDict[key]
    for key in errDict: errDict[key] = resultDict[key] + errDict[key]
    # 对normDict.values()二维数组按照Index Recommend Type即第8个数组进行排序
    normList = normDict.values()
    normList.sort(key=lambda x: x[-1], reverse=True)
    top_sql_info = normList + errDict.values()
    # 记录topsql个数
    top_sql_count = len(normList) + len(errDict.values())
    # 记录可用的索引建议个数
    advis_idx_count = len(goodAdvisList)
    sqlinfo = []
    for key in errDict:
        sqlinfo.append([key, sql_md5_ref[key]])
    for key in normDict:
        sqlinfo.append([key, sql_md5_ref[key]])
    del normDict
    del errDict
    del sql_md5_ref
    del normList
    rows = []
    for row in goodAdvisList:
        format_index = row[2]
        tabschema = getNameFromIdx(format_index)[0]
        tabname = getNameFromIdx(format_index)[1]
        row_tab = []
        row_idx = []
        row_tab_str = ''
        row_idx_str = ''
        for tab_row in db2_tab_list:
            if tabschema == tab_row[0] and tabname == tab_row[1]:
                row_tab = [tab_row]
                row_tab_str = html_table('Table Info', ['tabschema', 'tabname', 'type', 'Index Exists count',
                                                        'rows(based on runstats)', 'runstats time', 'create time'],
                                         row_tab)
                break
        for idx_row in db2_idx_list:
            if tabname == idx_row[3]:
                row_idx.append([idx_row[0], idx_row[1], idx_row[4], idx_row[5]])
        row_idx_str = html_table('Exists Indexes Info', ['Index schema', 'Index Name', 'Index Create Time', 'Columns'],
                                 row_idx)
        rows.append(row + [row_tab_str] + [row_idx_str])
    # TOP SQL Information
    html_topsql_info = html_table('', ['Executions', 'Total Rows Read', 'Total Exec Time', 'Total CPU Time',
                                       'Avg Rows Read', 'Avg Exec Time', 'Avg CPU Time', 'Avg Data Logical Reads',
                                       'Avg Data Physical Reads', 'Avg Index Logical Reads', 'Avg Index Physical Reads',
                                       'Tabschema', 'SQL Text', 'SQL MD5', 'Index Recommend Type'], top_sql_info)
    # Advis Information
    html_advis = html_table('', ['Improvement', 'Index Size', 'Create Index Text', 'SQL MD5 Chain', 'Table Information',
                                 'Exists Indexes Information'], rows, intervalflag=0)
    # SQL Information
    html_sqlinformation = html_table('', ['SQL MD5', 'Full SQL Text'], sqlinfo)
    logging.info("Generate report sucessfully !")


    # 打印html报告
    def html_concat(str):
        global html_str
        html_str = html_str + '\n' + str


    html_str = ''
    buttions = '''
    <div name="buttons">
    <input name="Overview_but" value="Overview" onclick="location.href='#Overview'" readonly="readonly" style="font-weight: bold" type="submit">
    <input name="topsqlinfo_but" value="TOP SQL Information " onclick="location.href='#topsqlinfo'" readonly="readonly" style="font-weight: bold" type="submit">
    <input name="advisinfo_but" value="Advis Information" onclick="location.href='#advisinfo'" readonly="readonly" style="font-weight: bold" type="submit">
    <input name="sqlinfo_but" value="SQL Information" onclick="location.href='#sqlinfo'" readonly="readonly" style="font-weight: bold" type="submit">
    </div>
    '''
    html_concat('<a name="main"></a>')
    back_to_top_div = r'<div name="backto"><p align="right"><a href="#main">Back to the top of the report</a></p></div> <hr>'
    html_concat(html_title('SQL Tuning Recommendation Summary', title_level='h1', title_pos='center'))
    html_concat(buttions)
    html_concat('<br>')
    html_concat('<h2 name="Overview" align="LEFT">Overview</h2>')
    html_concat('<ul>')
    html_concat('<li><b>Timestamp for the recommendations and analysis: </b>' + curtime + '<br></li>')
    html_concat('<li><b>Host Name: </b>' + hostname + '<br></li>')
    html_concat('<li><b>User Name: </b>' + user + '<br></li>')
    html_concat('<li><b>Database Name: </b>' + dbname + '<br></li>')
    html_concat('<li><b>Number of TOP SQLs : </b>' + str(top_sql_count) + '<br></li>')
    html_concat('<li><b>Number of Advised Indexes : </b>' + str(advis_idx_count) + '</li>')
    html_concat('</ul>')
    html_concat('<br>')
    html_concat('<hr>')
    html_body_str = html_str + html_title('TOP SQL Information:', title_level='h2', title_pos='left', blanks=2,
                                          name='topsqlinfo') + html_topsql_info + back_to_top_div + html_title(
        'Advis Information:', title_level='h2', title_pos='left', blanks=2,
        name='advisinfo') + html_advis + back_to_top_div + html_title('SQL Information:', title_level='h2',
                                                                      title_pos='left', blanks=2,
                                                                      name='sqlinfo') + html_sqlinformation + back_to_top_div
    try:
        with open(html_outputfile, 'w') as f:
            f.write(html_head() + html_body(html_body_str))
    except IOError as e:
        print e
    fw.close()
    f.close()
    logging.info("Report file name: " + html_outputfile)



