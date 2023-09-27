from os import listdir, chdir
from os.path import isfile, join
from itertools import groupby

import json
import gc
import pandas as pd
import numpy as np
import re

PATH = "./data/"
DATA = "news_29_80k"


def word_finder(text: str) -> int:
    """Ф-я поиска кол-ва слов в тексте"""
    link_process = re.sub(r"http://\S+|https://\S+",
                          "", text)  # Удаление ссылок
    # Удаление мусора (оставляем слова и цифры)
    garbage_process = re.sub(r'([^\s\w])', '', link_process)
    return len(re.sub(r'\w*\d+\w*', ' ', garbage_process).split())


def only_smile(text: str) -> int:
    """Удаление всего, кроме смайлов"""
    del_space = re.sub(
        '\s+', ' ', text).strip().lower()  # Удаление пробелов, перевод регистра
    del_words = re.sub(r'\w', '', del_space)
    del_symbols = len(
        (re.sub(r"[~.,?!{}#%№+$^&*:""+/{};|]", '', del_words)).split())
    return del_symbols


files = [f.split(".")[0].split(" ") for f in listdir(
    f"{PATH}{DATA}") if isfile(join(f"{PATH}{DATA}", f))]
files = sorted([files[0][::-1] for files[0] in files], key=lambda x: x[0])
themes_channels = {int(k): [b for a, b in g]
                   for k, g in groupby(files, key=lambda x: x[0])}

our_data = pd.DataFrame(columns=["id", "channel", "text", "genre"])

data = ""

for theme in themes_channels:
    for channel in themes_channels[theme]:
        if f"{channel} {theme}.json" in listdir(f"{PATH}{DATA}"):
            with open(f"{PATH}{DATA}/{channel} {theme}.json", encoding="utf8") as f:
                for line in f:
                    data = json.loads(line)
                    gc.collect()
            print(f"{channel} {theme}.json OK")
        for i in range(len(data)):
            try:
                our_data.loc[len(our_data.index)] = [int(
                    (data[i]["id"])), channel, data[i]["message"].replace(",", "."), theme]

            except KeyError:
                our_data.loc[len(our_data.index)] = [int(
                    data[i]["id"]), channel, np.nan, theme]


our_data = our_data.dropna().reindex()

for index in our_data.index:
    text = our_data.loc[index, 'text']
    word_count = word_finder(text)  # Нашли кол-во слов
    sign_count = len(re.findall(r'!', text))  # Нашли кол-во знаков '!'
    smile_count = only_smile(text)  # Нашли кол-во смайлов
    our_data.loc[index, ['word_count', 'sign_count', 'smile_count']] = [
        word_count, sign_count, smile_count]


our_data.to_csv(path_or_buf=f"{PATH}/dataset/{DATA}.csv", index=False, sep=",")
