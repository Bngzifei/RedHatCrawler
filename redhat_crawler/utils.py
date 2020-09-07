# -*- coding: utf-8 -*-
import functools
import time


def time_it(fn):
    @functools.wraps(fn)
    def inner(*args, **kwargs):
        start = time.time()
        fn(*args, **kwargs)
        end = time.time()
        print("Time cost for function `{}`: {}".format(fn.__name__, (end - start)))

    return inner


def retry(**kw):
    def wrapper(func):
        def _wrapper(*args, **kwargs):
            raise_ex = None
            for num in range(kw['reNum']):
                print("This is the {number}-time of requests".format(number=str(num + 1)))
                try:
                    return func(*args, **kwargs)
                except Exception as ex:
                    raise_ex = ex
            print(raise_ex)

        return _wrapper

    return wrapper


def dict2str(dict_obj):
    """字典格式转成字符串格式"""
    assert isinstance(dict_obj, dict)
    cookie_elements = list()
    for k, v in dict_obj.items():
        each_obj = "=".join([k, v])
        cookie_elements.append(each_obj)

    element_str = ";".join(cookie_elements)
    return element_str
