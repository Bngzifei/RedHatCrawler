# -*- coding: utf-8 -*-
import re
import os

SRC_FILE_PATH = r"/sf/data/local/TESTCASE/HW_Platform/test_script/rhel8_changelog/"
SRC_FILE_PATH7 = r"/sf/data/local/TESTCASE/HW_Platform/test_script/rhel7_changelog/"

DATE_PATTERN = r"([0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|[1-9][0-9]{3})-(((0[13578]|1[02])-(0[1-9]|[12][0-9]|3[01]))|((0[469]|11)-(0[1-9]|[12][0-9]|30))|(02-(0[1-9]|[1][0-9]|2[0-8])))"
CATEGORY_PATTERN = r"- (\[)\w+(\])"

# DST_FILE_PATH_PREFIX = r"/sf/data/local/TESTCASE/HW_Platform/test_script/rhel8/"
DST_FILE_PATH_PREFIX = r"/sf/data/local/TESTCASE/HW_Platform/test_script/rhel7/"


# DST_CATEGORIES_DIRS = r"/sf/data/local/TESTCASE/HW_Platform/test_script/rhel8/"

class RHELChangeLogHandler:

    def __init__(self, dir_path):
        self.dir_path = dir_path

    def get_all_categories(self):
        """获取所有种类的目录"""
        all_categories = list()
        for dir_path, dir_names, filenames in os.walk(self.dir_path):
            for dir_name in dir_names:
                all_categories.append(dir_name)
        return all_categories

    def make_category_dir_by_rhel(self):
        """根据rhel文本中的日志类别创建目录"""
        for dir_path, dir_names, filenames in os.walk(self.dir_path):
            for filename in filenames:
                file = "".join([self.dir_path, filename])
                with open(file, "r") as fp:
                    datas = fp.readlines()
                    for item in datas:
                        item = item.strip("\n")
                        item = item.strip()
                        # res_title = re.match(DATE_PATTERN, item)
                        res_category = re.match(CATEGORY_PATTERN, item)
                        if res_category:
                            category_dir = res_category.group().lstrip(
                                "- ").lstrip("[").rstrip("]")
                            CATEGORY_FILE_PATH = "".join(
                                [DST_FILE_PATH_PREFIX, category_dir])
                            if not os.path.exists(CATEGORY_FILE_PATH):
                                os.makedirs(CATEGORY_FILE_PATH)

    def get_element_index_by_rhel_file_txt(self):
        """根据rhel文本获取日期(当做标题)及日志内容的索引位置"""
        file_index_map = dict()
        for dir_path, dir_names, filenames in os.walk(self.dir_path):
            for filename in filenames:
                file = "".join([self.dir_path, filename])
                title_indexs = list()
                with open(file, "r") as fp:
                    datas = fp.readlines()
                    for item in datas:
                        obj = item.strip("\n")
                        obj = obj.strip()
                        res_title = re.match(DATE_PATTERN, obj)
                        if res_title:
                            title_index = datas.index(item)
                            # res_title_index_map = {
                            #     res_title:
                            # }
                            title_indexs.append(title_index)
                            # import pdb;pdb.set_trace()
                            res_category_obj = datas[title_index + 1]

                file_index_map[filename] = title_indexs
        return file_index_map

    # get_element_index_by_rhel_file_txt(SRC_FILE_PATH)
    def trans_each_changelog_file_data_format_by_rhel_txt(self):
        """根据指定的rhel文本内容构建数据格式:
            返回数据格式:
            title_objs =
            [
                {标题一:[数据一,数据二,...]},
                {标题二:[数据一,数据二,...]},
                {标题三:[数据一,数据二,...]},
                ...
                ...
            ]
            单个文件的情况
        """
        pass

    def trans_changelog_file_data_format_by_rhel_txt(self):
        """根据rhel文本内容构建数据格式:
        返回数据格式:
        title_objs =
        [
            {标题一:[数据一,数据二,...]},
            {标题二:[数据一,数据二,...]},
            {标题三:[数据一,数据二,...]},
            ...
            ...
        ]
        all_log_file_datas = {
            "文件一":title_objs,
            "文件二":title_objs,
            ...
        }
        """
        file_index_map = self.get_element_index_by_rhel_file_txt()
        all_log_file_datas = dict()
        for dir_path, dir_names, filenames in os.walk(self.dir_path):
            for filename in filenames:
                file = "".join([self.dir_path, filename])
                # file = "".join([SRC_FILE_PATH, "changelog-2020-06-30-10-44-48-4.18.0-80.el8999.txt"])
                with open(file, "r") as fp:
                    datas = fp.readlines()
                    current_title_indexes = file_index_map[filename]
                    # current_title_indexes = file_index_map["changelog-2020-06-30-10-44-48-4.18.0-80.el8999.txt"]
                    print(current_title_indexes)
                    # import pdb;pdb.set_trace()
                    # [2, 4, 13, 15, 21, 25, 33, 60, 71, 73]
                    # 先获取当前的更新日志标题:
                    # [{标题一:[数据一,数据二,...]}, {标题一:[数据一,数据二,...]},... ]
                    title_objs = list()
                    times = len(current_title_indexes)
                    i = 0
                    #########问题出在这里:双层循环导致的数据不一致################
                    # [2, 4, 13]
                    for title_index in current_title_indexes:

                        if i < times - 1:
                            # {标题一:[数据一,数据二,...]}
                            title_data = dict()
                            # 文本中的每一个标题
                            title = datas[title_index]
                            # 取出title就把空格去掉
                            title = title.strip("\n").strip()
                            # 下一个标题的索引
                            next_title_index = current_title_indexes.index(
                                title_index) + 1
                            # 下一个日志内容的索引
                            next_log_obj_index = current_title_indexes[
                                next_title_index]
                            # 日志内容
                            log_items = datas[
                                        (title_index + 1):next_log_obj_index]
                            title_data[title] = [item.strip() for item in
                                                 log_items]
                            title_objs.append(title_data)

                        if i == times - 1:
                            # 说明是最后一个标题
                            last_title_data = dict()
                            last_index = current_title_indexes[i]
                            last_items = list()
                            for item in datas[(last_index + 1):]:
                                item = item.strip()
                                last_items.append(item)
                            last_title = datas[last_index]
                            # 取出title就把空格去掉
                            last_title = last_title.strip("\n").strip()
                            last_title_data[last_title] = last_items
                            title_objs.append(last_title_data)

                        i += 1
                    # 单个文件的title_objs
                    all_log_file_datas[filename] = title_objs

        return all_log_file_datas


    # make_changelog_file_by_rhel_txt(SRC_FILE_PATH)
    def get_all_files_name(self):
        """获取所有文件名称"""
        for dir_path, dir_names, filenames in os.walk(self.dir_path):
            return filenames

    def get_single_file_title(self, file, filename):
        """获取单个文件的所有标题"""
        with open(file, "r") as fp:
            datas = fp.readlines()
            items = list()
            for data in datas:
                obj = data.strip("\n")
                obj = obj.strip()
                items.append(obj)
            file_index_map = self.get_element_index_by_rhel_file_txt()
            current_title_indexes = file_index_map[filename]
            titles = list()
            for title_index in current_title_indexes:
                # 在这里将标题的空格和\n去掉
                title = items[title_index]
                titles.append(title)

            return titles

    def get_res_category_obj_dir_path(self, title, res_category_objs):
        """获取日志内容的目录路径"""
        res_title = re.match(DATE_PATTERN, title)
        file_dirs = list()
        for res_category_obj in res_category_objs:
            category_obj = res_category_obj.strip("\n")
            category_obj = category_obj.strip()
            res_category = re.match(CATEGORY_PATTERN, category_obj)
            if res_category:
                category_dir = res_category.group().lstrip("- ").lstrip(
                    "[").rstrip("]")
                DST_FILE_PATH = "".join(
                    [DST_FILE_PATH_PREFIX, category_dir, "/",
                     res_title.group(), ".txt"])
                file_dirs.append(DST_FILE_PATH)
        return file_dirs

    def make_changelog_file_by_rhel_txt(self):
        """按照日志类别写入日志文件内容"""
        all_files_log_data = self.trans_changelog_file_data_format_by_rhel_txt()
        filenames = self.get_all_files_name()
        for filename in filenames:
            each_log_datas = all_files_log_data[filename]
            # import pdb;pdb.set_trace()
            # {标题一:[数据一,数据二,...]},
            for data in each_log_datas:
                title = [k for k in data.keys()][0]
                # import pdb;pdb.set_trace()
                res_category_objs = data[title]
                # res_title_obj = title.strip("\n").strip()
                res_title = re.match(DATE_PATTERN, title)
                # 去搜索
                file_dirs = self.get_res_category_obj_dir_path(title,
                                                               res_category_objs)
                for file_dir in file_dirs:
                    # 先写入标题,再去写内容
                    with open(file_dir, "w") as f:
                        f.write(title + "\n")
                for res_category_obj in res_category_objs:
                    category_obj = res_category_obj.strip("\n")
                    category_obj = category_obj.strip()
                    res_category = re.match(CATEGORY_PATTERN, category_obj)
                    if res_category:
                        category_dir = res_category.group().lstrip(
                            "- ").lstrip(
                            "[").rstrip("]")
                        DST_FILE_PATH = "".join(
                            [DST_FILE_PATH_PREFIX, category_dir, "/",
                             res_title.group(), ".txt"])
                        with open(DST_FILE_PATH, "a") as f:
                            f.write("".join(["  ", res_category_obj, "\n"]))


def main():
    log_handler = RHELChangeLogHandler(SRC_FILE_PATH7)
    # all_log_file_datas = log_handler.trans_changelog_file_data_format_by_rhel_txt()
    log_handler.make_category_dir_by_rhel()
    log_handler.make_changelog_file_by_rhel_txt()


if __name__ == '__main__':
    main()