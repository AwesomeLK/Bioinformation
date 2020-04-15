# -*- coding = utf-8 -*-
'''
    数据处理总文件

    Author:likun
    Email:likun@megagenomics.cn
'''
import tools
import sys

# 传递待分析批次路径、样本明细表
path_list = sys.argv
main_path = path_list[1]
sample_file = path_list[2]

# main_path = '/Users/likun/Desktop/test_data/2020-03-10'
# sample_file = '20200310第8次上机样本明细.xls'
excel_name = tools.data_grabbing(main_path, sample_file)


tools.data_calculation(excel_name)
