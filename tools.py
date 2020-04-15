# -*- coding:utf-8 -*-
'''
    数据拆分过滤后，分析处理工具合集。

    Author:likun
    Time: 2020/03/20
    Email:likun@megagenomics.cn
'''

import os
import xlsxwriter
import pandas as pd

'''
    data_grabbing()
    
    功能说明：
    对同一批次下机数据进行抓取，生成一个初步数据表格，仅有一个sheet，sheet名为该函数名，表格名称为：
    'xxxx-xx-xx.xlsx'，如：'2020-03-10.xlsx'，函数最终会返回生成的表格名称，便于后续读取；
    与此同时，会在目录下生成样本信息明细多sheet合并表格，如有需要，可以查询，名称为：
    '批次日期_sample_collation.xlsx'，如：'20200310_sample_collation.xlsx'。
    
    参数详情：
    main_path = 待分析批次路径，如：'/Users/likun/Desktop/test_data/2020-03-10'（末尾不加'/'）；
    sample_file = 该批次样本信息明细表格名称，如：'20200310第8次上机样本明细.xls'。
    
    更新说明：
    2020/04/02更新：line 80 构建字典时，统一key为'str'格式。
    2020/04/06更新：line 137,145 增加了抓取样本是否在样本信息单中存在的判断。
    2020/04/07更新：
    1、line 71,153 细化了样本理想产出的分类；
    2、计算目标产出时，区别线上线下与只分析不出报告相同美因编号的情况。
    2020/04/08更新：line 133 增加数据文件类型筛选，指定'.stat'文件格式。
'''
def data_grabbing(main_path, sample_file):
    # 创建Excel文件并设计表头以及属性
    excel_name = main_path.split('/')[5] + '.xlsx'
    work_book = xlsxwriter.Workbook(excel_name)

    work_sheet = work_book.add_worksheet('Data_Grabbing')
    sheet_title = ['美因编号', '分析形式', 'Lane', '预期产出/M', '实际产出/M', 'Q20_fq1(%)', 'Q20_fq2(%)', 'Q30_fq1(%)',
                   'Q30_fq2(%)', '产出差值', '产出比(%)', '备注']
    i = 0
    for title in sheet_title:
        work_sheet.write(0, i, title)
        i += 1

    '''获取对应样本数据量'''
    # 读取多个sheet数据
    data_1 = pd.DataFrame(pd.read_excel(sample_file, sheet_name=0))
    data_2 = pd.DataFrame(pd.read_excel(sample_file, sheet_name=1))

    # 插入线上、线下、一代改二代以作区分
    data_1.insert(1, '分析形式', 'nextflow-lims')
    data_2.insert(1, '分析形式', 'nextflow')
    i = 0
    note_list = []
    for n_l in data_2['备注']:
        if n_l == 'NULL':
            n_l = '无'
        note_list.append(str(n_l))
    while i < len(data_2):
        if '只分析不出报告' in note_list[i] or '质控品' in note_list[i]:
            data_2.loc[i, '分析形式'] = 'nextflow-yidai'
        i += 1

    # 多个sheet数据合并
    data = data_1.append(data_2)

    # 删除'需求数据量'列
    del data['需求数据量(G)']

    # 按照'建库板号'进行需求数据量计算,并存储至列表中
    data_size = 0
    size_list = []
    for i in data['建库板号']:
        if 'NX' in i:
            data_size = 30 * 300 * 500 / 1024 / 1024
        elif '直扩' in i:
            data_size = 69 * 300 * 500 / 1024 / 1024
        elif '一代改二代' in i:
            data_size = 10 * 300 * 500 / 1024 / 1024
        elif 'N00' in i:
            data_size = 390 * 300 * 500 / 1024 / 1024
        else:
            data_size = 0

        size_list.append(data_size)

    # 添加所得的数据量并输出
    data.insert(15, '需求数据量(M)', size_list)
    file_name = sample_file[:8] + '_sample_collation.xlsx'
    data.to_excel(file_name, index=False)

    # 对美因编号和预期产出做成字典，并统一key为str类型
    data_list = []
    m_l = list(data['美因编号'])
    f_l = list(data['分析形式'])
    for i in range(len(data)):
        d_l = str(m_l[i]) + f_l[i]
        data_list.append(d_l)
    size = dict(zip(data_list, size_list))

    ''' 开始抓取样本信息及产出'''
    # 主循环，层层递归
    n = 1
    for form in os.listdir(main_path):

        # 对路径进行递归增加并设置判断
        fullpath = os.path.join(main_path, form)
        if form in ['nextflow', 'nextflow-lims', 'nextflow-yidai']:

            for line in os.listdir(fullpath):

                if line == 'nextflow':
                    Sample_fullpath = os.path.join(fullpath, line)

                    for sample_name in os.listdir(Sample_fullpath):

                        qc_path = os.path.join(Sample_fullpath, sample_name)

                        for line in os.listdir(qc_path):

                            if line == '01.lane_qc':
                                lane_path = os.path.join(qc_path, line)

                                for line in os.listdir(lane_path):

                                    qstat_path = os.path.join(lane_path, line)

                                    for qstat_filename in os.listdir(qstat_path):

                                        if os.path.splitext(qstat_filename)[1] == '.stat':

                                            file_path = os.path.join(qstat_path, qstat_filename)
                                            q_f = open(file_path, 'r', encoding='utf-8')
                                            print(file_path)

                                            # 获取目标文件数据
                                            line = q_f.readline()
                                            i = 1
                                            while i < 12:
                                                if i == 3:
                                                    qstat_size = int(line.split()[2]) / (1024 * 1024)
                                                if i == 8:
                                                    Q20_fq1 = line.split()[3]
                                                if i == 9:
                                                    Q20_fq2 = line.split()[3]
                                                if i == 10:
                                                    Q30_fq1 = line.split()[3]
                                                if i == 11:
                                                    Q30_fq2 = line.split()[3]
                                                else:
                                                    line = q_f.readline()
                                                i += 1

                                            # 部分数据计算
                                            data_dif = 0
                                            data_rate = 0
                                            mark_name = sample_name + form
                                            if mark_name in size:
                                                data_dif = float(qstat_size) - float(size[mark_name])
                                                if size[mark_name] == 0:
                                                    data_rate = 0
                                                else:
                                                    data_rate = float(qstat_size) / float(size[mark_name]) * 100

                                            # 写入Excel中
                                            work_sheet.write(n, 0, sample_name)
                                            work_sheet.write(n, 1, form)
                                            work_sheet.write(n, 2, qstat_path[-4:])
                                            if mark_name in size:
                                                work_sheet.write(n, 3, size[mark_name])
                                                if size[mark_name] == 0:
                                                    work_sheet.write(n, 11, '非常规样本')
                                            else:
                                                work_sheet.write(n, 3, 'NULL')
                                                work_sheet.write(n, 11, '样本信息单缺失')
                                            work_sheet.write(n, 4, qstat_size)
                                            work_sheet.write(n, 5, Q20_fq1[:-1])
                                            work_sheet.write(n, 6, Q20_fq2[:-1])
                                            work_sheet.write(n, 7, Q30_fq1[:-1])
                                            work_sheet.write(n, 8, Q30_fq2[:-1])
                                            work_sheet.write(n, 9, data_dif)
                                            work_sheet.write(n, 10, data_rate)

                                            # 修改行数
                                            n += 1
    work_book.close()
    return excel_name

'''
    data_calculation()

    功能说明：
    对于data_grabbing函数已获取的下机数据表格进行合并，在原有基础上增加第二个sheet，名称为该函数名；
    合并关键词顺序为'美因编号'→'分析形式'；
    计算产出差异和产出比。
    
    参数详情：
    excel_name = data_grabbing函数生成的数据表格名

    更新说明：
    暂无。
'''
def data_calculation(excel_name):
    # 读取Excel中Sheet1中的数据
    data = pd.DataFrame(pd.read_excel(excel_name, sheet_name=0))

    # 保留sheet1
    data_op = data.copy()

    # 新建空DF
    new_data = pd.DataFrame()

    # 列出需要合并项
    com_title = ['实际产出/M', '预期产出/M', 'Q20_fq1(%)', 'Q20_fq2(%)', 'Q30_fq1(%)', 'Q30_fq2(%)']
    divi_title = com_title[1:]

    # 列表化
    f_1 = data_op['美因编号']
    f_2 = data_op['分析形式']
    n = 0
    i = 0
    c = 1
    
    # 主循环
    while True:

        # 相邻行'美因编号'、'分析形式'同时判断
        if f_1[n] == f_1[n+1]:
            if f_2[n] == f_2[n+1]:
                c += 1

                # 对于需要处理数据先进行相加，并赋值给重复行的第一行
                for title in com_title:
                    data_op.loc[i, title] = float(data_op.loc[i, title]) + float(data_op.loc[n+1, title])

        # 相邻不同则对上述相加数据进行平均数的计算
        else:
            for title in divi_title:
                data_op.loc[i, title] = float(data_op.loc[i, title])/c

            # 导入新的DF中
            new_data = new_data.append(data_op[i:i+1], ignore_index=True)
            c = 1
            i = n + 1
        n += 1

        # 最后的数据处理，以及退循环
        if n == len(data_op)-1:
            for title in divi_title:
                data_op.loc[i, title] = float(data_op.loc[i, title])/c

            new_data = new_data.append(data_op[i:i+1], ignore_index=True)
            break

    # 格式整理
    del new_data['Lane']

    # 排除除数时0的情况
    i = 0
    while i < len(new_data):
        new_data.loc[i, '产出差值'] = float(new_data.loc[i, '实际产出/M']) - float(new_data.loc[i, '预期产出/M'])
        if new_data.loc[i, '预期产出/M'] == 0:
            new_data.loc[i, '产出比(%)'] = 0
        else:
            new_data.loc[i, '产出比(%)'] = float(new_data.loc[i, '实际产出/M']) / float(new_data.loc[i, '预期产出/M']) * 100
        i += 1

    # 覆盖原有表格
    writer = pd.ExcelWriter(excel_name)
    data.to_excel(writer, sheet_name='Data_Grabbing', index=False)
    new_data.to_excel(writer, sheet_name='Data_Calculation', index=False)
    writer.save()

    # 友情提示
    return print('Mission Completed.')
