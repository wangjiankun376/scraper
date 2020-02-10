#!/usr/bin/python
# -*- coding: UTF-8 -*-

import requests
import re
import pandas as pd
from bs4 import BeautifulSoup


class Crawler:

    def __init__(self, start_month="2017/01"):

        # url dict format: {eindex: (url, column_dict)}
        self._url_dict = {"企业商品价格指数":
                              ("http://data.eastmoney.com/cjsj/corporategoodspriceindex.aspx",
                               {"Month": 0, "企业商品价格指数(总指数)": 1})
                         }

        self._start_month = start_month


    # ----------- private methods ----------- #

    def _init_BS_object_from_url(self, url):

        r = requests.get(url)
        r.encoding = "gb2312"
        html = r.text
        return BeautifulSoup(html, features="lxml")


    def _get_chunk_size(self, bs):
        """
        count the size of chunk (one row) in table
        """
        chunk = 0
        node_tr = bs.find("tr", class_="firstTr")
        nodes_th = node_tr.find_all("th")

        def _count_column(node_th):
            if "colspan" in node_th.attrs: return int(node_th["colspan"])
            else: return 1

        return sum(list(map(_count_column, nodes_th)))


    def _count_page(self, bs):
        """
        :param bs: BS object initiated from base url
        count number of webpages for a single econ index
        """
        node_pagecount = bs.find("input", id="pagecount")
        num_page = int(node_pagecount["value"])

        return num_page


    def _get_webpage_text(self, url, node_type="td"):
        """
        :return: list of all entries in a single webpage (if there is a table)
        """
        bs = self._init_BS_object_from_url(url)
        texts = bs.find_all(node_type)  # find all table data nodes
        texts = list(map(BeautifulSoup.get_text, texts))  # remove tab labels and retrieve text only
        texts = list(map(str.strip, texts))  # remove spaces and other chars

        return texts


    def _divide_texts_in_chunks(self, texts, chunk_size):

        return [texts[i:i+chunk_size] for i in range(0, len(texts), chunk_size)]


    def _filter_chunks_entries(self, chunks, ind):
        """
        :param chunks: input list of data chunks
        :param ind: list of indices that are to be kept
        :return: filtered list of data chunks
        """
        return [[c[i] for i in ind] for c in chunks]      # list of lists


    def _format(self, chunks):

        # function of formatting date that contains Chinese characters
        # output date str formatted like "2017/01"
        def _format_date(date_str):
            str_no_chinese = re.sub("[\u4e00-\u9fa5]", "", date_str)
            return str_no_chinese[:4] + "/" + str_no_chinese[4:]

        # function of formatting a data chunk c: [date, data1, data2, ...]
        def _format_chunk(c):
            return tuple([_format_date(c[i]) if i == 0 else float(c[i])
                          for i in range(len(c))])

        return list(map(_format_chunk, chunks))



    def _crawl_page(self, url, chunk_size, ind):
        """
        :param url: url of one webpage
        :param chunk_size: number of entries in a row within the table in webpage
        :param ind: indices of items in a row that will be kept
        :return: 1. data
                 2. earliest month
        """
        texts = self._get_webpage_text(url)
        chunks = self._divide_texts_in_chunks(texts, chunk_size=chunk_size)
        filtered_chunks = self._filter_chunks_entries(chunks, ind)
        formatted_chunks = self._format(filtered_chunks)   # list of tuples
        earliest = filtered_chunks[-1][0]     # earliest month in current webpage

        return formatted_chunks, earliest


    def _list2df(self, zipped_data, columns):
        """
        :param zipped_data: list of tuples of data [(month1, data1_1, data1_2, ...),
                                                    (month2, data2_1, data2_2, ...),
                                                    ...]
        :param columns: column names
        :return: pandas dataframe
        """
        num_col = len(columns)
        data_dict = {columns[i]: list(zip(*zipped_data))[i] for i in range(num_col)}
        return pd.DataFrame(data=data_dict)


    # ----------- callable methods ----------- #

    def crawl_index(self, eindex):
        """
        column_dict: {k: v} where k is table header, v is column index
                     eg. {"Month": 0, "PMI(Manu)": 1, "PMI(Non-Manu)": 3}
        :param eindex: econ index (eg.企业商品价格指数)
        :return: pd dataframe with columns specified in column_dict
        """
        base_url, column_dict = self._url_dict[eindex]
        bs = self._init_BS_object_from_url(base_url)
        num_page = self._count_page(bs)
        chunk_size = self._get_chunk_size(bs)
        data = []

        ind, columns = [], []
        for k, v in column_dict.items():
            ind.append(v)
            columns.append(k)

        for i in range(1, num_page+1):

            url = base_url + "?p=%i"%i
            data_page, earliest = self._crawl_page(url, chunk_size, ind)
            data += data_page

            # if data in current page is old enough, stop the loop
            if earliest <= self._start_month: break

        df = self._list2df(data, columns)
        df = df.loc[df["Month"] >= self._start_month]

        return df



if __name__ == "__main__":

    c = Crawler()
    df = c.crawl_index("企业商品价格指数")
    print(df)