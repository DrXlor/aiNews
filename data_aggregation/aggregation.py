from os import listdir
from os.path import isfile, join
from itertools import groupby

import json
import gc
import pandas as pd
import numpy as np
import re


def word_counter(text: str) -> int:
    """A function to find number of words in a string

    Args:
        text (str): The sentence in which words are searched for

    Returns:
        int: The number of words in the string
    """
    link_process = re.sub(r"http://\S+|https://\S+", "", text)
    garbage_process = re.sub(r'([^\s\w])', '', link_process)
    return len(re.sub(r'\w*\d+\w*', ' ', garbage_process).split())


def emojies_counter(text: str) -> int:
    """A function to find number of emojies in a string

    Args:
        text (str): The sentence in which emojies are searched for

    Returns:
        int: _description_
    """
    del_space = re.sub(
        '\s+', ' ', text).strip().lower()
    del_words = re.sub(r'\w', '', del_space)
    del_symbols = len(
        (re.sub(r"[~.,?!{}#%№+$^&*:""+/{};|]", '', del_words)).split())
    return del_symbols


def aggregate(path: str, data: str) -> None:

    data = ""

    files = [f.split(".")[0].split(" ") for f in listdir(
        f"{path}{data}") if isfile(join(f"{path}{data}", f))]
    files = sorted([files[0][::-1] for files[0] in files], key=lambda x: x[0])

    themes_channels = {int(k): [b for a, b in g]
                       for k, g in groupby(files, key=lambda x: x[0])}

    our_data = pd.DataFrame(columns=["id", "channel", "text", "genre"])

    data = ""

    for theme in themes_channels:
        for channel in themes_channels[theme]:
            if f"{channel} {theme}.json" in listdir(f"{path}{data}"):
                with open(f"{path}{data}/{channel} {theme}.json", encoding="utf8") as f:
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
        word_count = word_counter(text)
        sign_count = len(re.findall(r'!', text))
        smile_count = emojies_counter(text)
        our_data.loc[index, ['word_count', 'sign_count', 'smile_count']] = [
            word_count, sign_count, smile_count]

    our_data.to_csv(
        path_or_buf=f"{path}/dataset/{data}.csv", index=False, sep=",")
