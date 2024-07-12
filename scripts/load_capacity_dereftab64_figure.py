import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.ticker import LinearLocator
import os
import re

exp_dir = ".."

RES_PATH = exp_dir + '/results'
FIGURE_FOLDER = RES_PATH + '/figure'


def ReadCPUTimefromFile(file_name):
    with open(file_name, "r") as fp:
        lines = fp.readlines()
        for line in lines:
            if len(re.findall(r"CPU Time", line)):
                return int(re.findall(r"\d+", line)[0])


def ReadTableSizefromFile(file_name):
    with open(file_name, "r") as fp:
        lines = fp.readlines()
        for line in lines:
            if len(re.findall(r"Table Size", line)):
                return int(re.findall(r"\d+", line)[0])


def ReadLoadCapacityfromFile(file_name):
    with open(file_name, "r") as fp:
        lines = fp.readlines()
        for line in lines:
            if len(re.findall(r"Load Capacity", line)):
                return int(re.findall(r"\d+", line)[0])


def GetResFileName(object_id, case_id, entry_id):
    return RES_PATH + "/" + "object_" + str(object_id) + "_case_" + str(case_id) + "_entry_" + str(entry_id) + "_.txt"


def GetResPerfFileName(object_id, case_id, entry_id):
    return RES_PATH + "/" + "object_" + str(object_id) + "_case_" + str(case_id) + "_entry_" + str(entry_id) + "_perf.txt"


def ReadCPUTime(rep_step, entry_id_base):
    res = []
    for table_size in [10000, 100000, 1000000, 10000000, 100000000]:
        col = []
        for rep_cnt in range(rep_step):
            col.append(ReadCPUTimefromFile(
                GetResFileName(0, 0, entry_id_base)))
        res.append(np.mean(col))


def ReadLoadCapacity(rep_step, entry_id_base):
    res = []
    for table_size in [10000, 100000, 1000000, 10000000, 100000000]:
        col = []
        for rep_cnt in range(rep_step):
            col.append(ReadLoadCapacityfromFile(
                GetResFileName(0, 0, entry_id_base)))
            entry_id_base = entry_id_base + 1
        res.append(np.mean(col))
    return res


def DrawFigure(x_value, y_value, x_lable, y_lable, filename):
    fig = plt.figure(figsize=(5, 3))
    figure = fig.add_subplot(111)

    plt.bar(range(len(x_value)), y_value)
    plt.xticks(range(len(x_value)), ['{:.0e}'.format(i) for i in x_value])

    for p in figure.patches:
        figure.annotate(text=np.round(p.get_height(), decimals=4),
                        xy=(p.get_x()+p.get_width()/2., p.get_height()),
                        ha='center',
                        va='center',
                        xytext=(0, 10),
                        textcoords='offset points')

    plt.grid(axis='y', color='gray', linestyle='--', alpha=0.5)

    # figure.yaxis.set_major_locator(LinearLocator())
    figure.set_ylim([0.9, 1])
    # figure.set_xscale('log')

    plt.xlabel(x_lable)
    plt.ylabel(y_lable)
    fig.tight_layout()

    plt.savefig(FIGURE_FOLDER + '/' + filename + '.pdf')


if __name__ == "__main__":

    rep_step = 5
    entry_id_base = 0
    object_id = 0
    case_id = 0

    legend_labels = []
    x_value = [10000, 100000, 1000000, 10000000, 100000000]
    y_value = ReadLoadCapacity(rep_step, entry_id_base)
    print(y_value)
    y_value = np.array(y_value) / np.array(x_value)
    print(y_value)

    DrawFigure(x_value, y_value, "Table Size", "Maximum Load Factor",
               "load_capacity_dereftab64_figure")
