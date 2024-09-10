import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.ticker import LinearLocator
from matplotlib import colors as mcolors


import os
import re

exp_dir = ".."

RES_PATH = exp_dir + '/results'
FIGURE_FOLDER = RES_PATH + '/figure'


MARKERS = ('o', 's', 'v', "^", "h", "v", ">", "x", "d", "<", "|", "", "|", "_")
COLORS = plt.get_cmap('Set1')
PATTERNS = ("", "\\\\", "//////", "o", "||", "\\\\", "\\\\",
            "//////", "//////", ".", "\\\\\\", "\\\\\\")


def ReadCPUTimefromFile(file_name):
    with open(file_name, "r") as fp:
        lines = fp.readlines()
        for line in lines:
            if len(re.findall(r"CPU Time", line)):
                return int(re.findall(r"\d+", line)[0])


def GetResFileName(object_id, case_id, entry_id):
    return RES_PATH + "/" + "object_" + str(object_id) + "_case_" + str(case_id) + "_entry_" + str(entry_id) + "_.txt"


def GetResPerfFileName(object_id, case_id, entry_id):
    return RES_PATH + "/" + "object_" + str(object_id) + "_case_" + str(case_id) + "_entry_" + str(entry_id) + "_perf.txt"


def ReadCPUTime(rep_step, entry_id_base):
    res = []
    # for case_id in range(1, 8):
    for case_id in range(1, 2):
        obj_list = []
        for object_id in range(0, 5):
            table_size_list = []
            # for table_size in [10000, 100000, 1000000, 10000000]:
            for table_size in [10000, 60000, 100000, 1000000, 10000000]:
                opt_num_list = []
                # for opt_num in [100000, 1000000, 10000000, 100000000]:
                for opt_num in [9000, 50000, 90000, 900000, 9000000]:
                    col = []
                    for rep_cnt in range(rep_step):
                        # if object_id < 4 and case_id != 5:
                        if table_size > opt_num:
                            col.append(ReadCPUTimefromFile(
                                GetResFileName(object_id, case_id, entry_id_base)))
                            entry_id_base = entry_id_base + 1
                        else:
                            col.append(0)
                    # if object_id < 4 and case_id != 5:
                    if True:
                        opt_num_list.append(np.mean(col))
                # if object_id < 4 and case_id != 5:
                if True:
                    table_size_list.append(opt_num_list)
            # if object_id < 4 and case_id != 5:
            if True:
                obj_list.append(table_size_list)
        res.append(obj_list)
    return res


def DrawFigure(x_value, y_value, x_lable, y_lable, filename, case_name):
    fig = plt.figure(figsize=(12, 3))
    figure = fig.add_subplot(111)

    # table_size_list = [10000, 100000, 1000000, 10000000]
    # opt_num_list = [100000, 1000000, 10000000, 100000000]

    table_size_list = [10000, 60000, 100000, 1000000, 10000000]
    opt_num_list = [9000, 50000, 90000, 900000, 9000000]
    len_opt_num_list = len(opt_num_list)

    object_name_list = ["DEREFTAB64",
                        "CHAINEDHT64",
                        "INTARRAY64",
                        "STDUNORDEREDMAP64",
                        "BYTEARRAYCHAINEDHT"]

    # for i in range(4):
    for i in range(5):
        for j in range(len(table_size_list)):
            plt.plot(range((len_opt_num_list + 1)*j, (len_opt_num_list + 1)
                     * j + len_opt_num_list), y_value[i][j], marker=MARKERS[i], color=COLORS(i))

    # for i in range(4):
    for i in range(5):
        plt.plot([], [], marker=MARKERS[i],
                 color=COLORS(i), label=object_name_list[i])

    x_range = []
    x_ticks = []
    for j in range(len(table_size_list)):
        x_range = x_range + list(range((len_opt_num_list + 1)*j, (len_opt_num_list + 1)
                                       * j + len_opt_num_list))
        x_ticks = x_ticks + \
            ['{:.0e}'.format(i) for i in opt_num_list]

    x_range = x_range + [(len_opt_num_list - 1) / 2 + (len_opt_num_list + 1)*i for i in range(
        len(table_size_list))]
    x_ticks = x_ticks + ['\n' + str('{:.0e} (table size)'.format(i))
                         for i in table_size_list]

    plt.xticks(x_range, x_ticks)

    # plt.xticks(range(len(x_value)), ['{:.0e}'.format(i) for i in x_value])

    for p in figure.patches:
        figure.annotate(text=np.round(p.get_height(), decimals=4),
                        xy=(p.get_x()+p.get_width()/2., p.get_height()),
                        ha='center',
                        va='center',
                        xytext=(0, 10),
                        textcoords='offset points')

    plt.grid(axis='y', color='gray', linestyle='--', alpha=0.5)

    # figure.yaxis.set_major_locator(LinearLocator())
    # figure.set_ylim([0.9, 1])
    # figure.set_xscale('log')

    plt.xlabel(x_lable)
    plt.ylabel(y_lable)
    plt.title(case_name)

    plt.legend(loc='best')

    fig.tight_layout()

    plt.savefig(FIGURE_FOLDER + '/' + filename + '.pdf')


if __name__ == "__main__":

    rep_step = 1
    entry_id_base = 0
    object_id = 0
    case_id = 0

    legend_labels = []
    # x_value = [10000, 100000, 1000000, 10000000, 100000000]
    x_value = [10000, 60000, 100000, 1000000, 1000000]
    y_value = ReadCPUTime(rep_step, entry_id_base)
    # print(y_value)

    case_name_list = ["INSERT_ONLY_LOAD_FACTOR_SUPPORT",
                      "INSERT_ONLY",
                      "UPDATE_ONLY",
                      "ERASE_ONLY",
                      "ALTERNATING_INSERT_ERASE",
                      "ALL_OPERATION_RAND",
                      "QUERY_HIT_ONLY",
                      "QUERY_MISS_ONLY",
                      "QUERY_HIT_PERCENT",
                      "QUERY_HIT_ONLY_CUSTOM_LOAD_FACTOR",
                      "QUERY_MISS_ONLY_CUSTOM_LOAD_FACTOR",
                      "QUERY_HIT_PERCENT_CUSTOM_LOAD_FACTOR"]

    # for case_id in range(1, 8):
    for case_id in range(1, 2):
        if case_id != 5:
            DrawFigure(x_value, y_value[case_id - 1], "#Operation\nTable Size", "CPU Time (ms)",
                       "cpu_time_case_" + str(case_id) + "_figure", case_name_list[case_id])

    # table_size_list = [10000, 100000, 1000000, 10000000]
    # opt_num_list = [100000, 1000000, 10000000, 100000000]
    table_size_list = [10000, 60000, 100000, 1000000, 10000000]
    opt_num_list = [9000, 50000, 90000, 900000, 9000000]

    # for case_id in range(1, 8):
    for case_id in range(1, 1):
        # for obj in range(4):
        for obj in range(5):
            if case_id != 5:
                for j in range(5):
                    for k in range(5):
                        if y_value[case_id - 1][obj][j][k] == 0:
                            continue
                        if case_id != 4:
                            y_value[case_id - 1][obj][j][k] = opt_num_list[k] / \
                                y_value[case_id - 1][obj][j][k]
                        else:
                            y_value[case_id - 1][obj][j][k] = table_size_list[j] / \
                                y_value[case_id - 1][obj][j][k]
    # for case_id in range(1, 8):
    for case_id in range(1, 2):
        if case_id != 5:
            DrawFigure(x_value, y_value[case_id - 1], "#Operation\nTable Size", "Throughput (#/ms)",
                       "throughput_case_" + str(case_id) + "_figure", case_name_list[case_id])
