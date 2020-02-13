#!/usr/bin/python
# -*- coding: UTF-8 -*-

import requests
import json
import pandas as pd
import numpy as np
from urllib import parse
from functools import reduce
from Crawler import Crawler


class WebScraper:

    def __init__(self, start_month="2017/01"):

        # 其他网页
        self._crawler = Crawler(start_month=start_month)

        # 国家统计局网页
        self._url = "http://data.stats.gov.cn/easyquery.htm?"
        self._sj_start = start_month.replace("/", "")
        self._index_dict = {"制造业采购经理指数(%)": "A0B0101",
                            "非制造业商务活动指数(%)": "A0B0201",
                            "工业生产者出厂价格指数(上年同月=100)": "A01080101",
                            "工业增加值累计增长(%)": "A020102",
                            "房地产投资累计值(亿元)": "A060101",
                            "流通中现金(M0)供应量期末值(亿元)": "A0D0105",
                            "货币(M1)供应量期末值(亿元)": "A0D0103",
                            "货币和准货币(M2)供应量期末值(亿元)": "A0D0101",
                            "GDP": "A010201",

                            # "生产指数": "A0B0102",
                            # "新订单指数": "A0B0103",
                            # "新出口订单指数": "A0B0104",
                            # "在手订单指数": "A0B0105",
                            # "产成品库存指数": "A0B0106",
                            # "采购量指数": "A0B0107",
                            # "进口指数": "A0B0108",
                            # "出厂价格指数": "A0B0109",
                            # "主要原材料购进价格指数": "A0B010A",
                            # "原材料库存指数": "A0B010B",
                            # "从业人员指数": "A0B010C",
                            # "供应商配送时间指数": "A0B010D",
                            # "生产经营活动预期指数": "A0B010E",
                            #
                            # "新订单指数": "A0B0202",
                            # "新出口订单指数": "A0B0203",
                            # "在手订单指数": "A0B0204",
                            # "存货指数": "A0B0205",
                            # "投入品价格指数": "A0B0206",
                            # "销售价格指数": "A0B0207",
                            # "从业人员指数": "A0B0208",
                            # "供应商配送时间指数": "A0B0209",
                            # "业务活动预期指数": "A0B020A",
                            #
                            # "综合PMI产出指数": "A0B0301"
                            }

        self._data_by_quarter = ["GDP"]


    # ----------- private methods ----------- #

    def _extract_json(self, jd):
        """
        :param jd: loaded json data
        :return: two lists (index data & month)
        """
        d = {"A": ["01", "02", "03"], "B": ["04", "05", "06"],
             "C": ["07", "08", "09"], "D": ["10", "11", "12"]}
        datanode_list = jd["returndata"]["datanodes"]
        data_list, month_list = [], []

        for node in datanode_list:

            hasdata = node["data"]["hasdata"]
            data = node["data"]["data"] if hasdata else np.nan
            time = node["wds"][1]["valuecode"]

            # data by month
            if time[-1].isdigit():
                year, month = time[:4], time[4:]
                time = year + "/" + month
                data_list.append(data)
                month_list.append(time)

            # data by quarter
            else:
                year, quarter = time[:4], time[4:]
                months = d[quarter]    # list of months
                data_list.extend([data] * len(months))
                time = [year + "/" + month for month in months]
                month_list.extend(time)

        return data_list, month_list


    def _get_url(self, key, sj_start):
        """
        one query can only contain one single index
        :param zb: target index
        :param sj_start: first month of data record (eg.201701)
        :return: html-encoded url for http request
        """
        zb = self._index_dict[key]
        if key in self._data_by_quarter: dbcode = "hgjd"
        else: dbcode = "hgyd"

        query = {"m": "QueryData",
                 "dbcode": dbcode,
                 "rowcode": "zb",
                 "colcode": "sj",
                 "wds": [],
                 "dfwds": [{"wdcode": "zb", "valuecode": zb},
                           {"wdcode": "sj", "valuecode": sj_start + "-"}]
                 }
        query_string = parse.urlencode(query, quote_via=parse.quote)
        url = self._url + query_string
        url = url.replace("%27", "%22")  # replace '' by "" in encoded string

        return url


    def _get_json_data(self, url):

        web_data = requests.get(url).text
        return json.loads(web_data)


    def _scrape(self, key):

        if key in ["企业商品价格指数"]:
            return self._crawler.crawl_index(key)
        else:

            url = self._get_url(key, self._sj_start)
            json_data = self._get_json_data(url)
            data_list, month_list = self._extract_json(json_data)
            data_dict = {"Month": month_list, key: data_list}
            return pd.DataFrame(data=data_dict)


    # ----------- callable methods ----------- #

    def scrape(self, index_key):

        if isinstance(index_key, str):
            return self._scrape(index_key)

        elif isinstance(index_key, list):
            _scrape = lambda key: self._scrape(key)
            dfs = list(map(_scrape, index_key))
            _merge = lambda df1, df2: pd.merge(df1, df2, on="Month", how="outer")
            return reduce(_merge, dfs)

        else:
            raise ValueError("Unsupported argument type, use string or list")



if __name__ == "__main__":

    s = WebScraper()
    df = s.scrape(["GDP", "制造业采购经理指数(%)"])
    print(df)
