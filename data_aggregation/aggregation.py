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


def clean_data(data: pd.DataFrame) -> pd.DataFrame:
    """Clean dataset by removing special, escape and punct symbols

    Args:
        data (pd.DataFrame): initial dataset the cleaning is going to be applied to

    Returns:
        pd.DataFrame: clean dataset
    """

    def symbols_rm(text: str) -> None:
        try:
            text = re.sub(r"http://\S+|https://\S+", "", text)
            text = re.sub(r'([^\s\w])', '', text).lower()
            text = re.sub(r'\w*\d+\w*', ' ', text).strip()
            text = re.sub('\s+', ' ', text)
            return re.sub(r'\n', '', text)
        except TypeError:
            pass

    data["text"] = data["text"].apply(symbols_rm)

    return data


def aggregate(PATH: str, DATASET: str) -> None:
    """A function that combines text from json files into unified pd.DataFrame
       with two extra features added in the process: number of words, number of '!' and number of emojies.
       Creates a new csv file, that stores combined dataset.

    Args:
        PATH (str): path to directory that contains data 
        DATASET (str): folder that contains jsons with exportet messages

    Returns:
        None
    """

    data = ""

    files = [f.split(".")[0].split(" ") for f in listdir(
        f"{PATH}{DATASET}") if isfile(join(f"{PATH}{DATASET}", f))]

    files = sorted([files[0][::-1] for files[0] in files], key=lambda x: x[0])
    themes_channels = {int(k): [b for a, b in g]
                       for k, g in groupby(files, key=lambda x: x[0])}
    our_data = pd.DataFrame(columns=["id", "channel", "text", "genre"])

    data = ""

    for theme in themes_channels:
        for channel in themes_channels[theme]:
            if f"{channel} {theme}.json" in listdir(f"{PATH}{DATASET}"):
                with open(f"{PATH}{DATASET}/{channel} {theme}.json", encoding="utf8") as f:
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

    clean_data(our_data).to_csv(
        path_or_buf=f"{PATH}/dataset/{DATASET}.csv", index=False, sep=",")


if __name__ == "__main__":
    aggregate(PATH="data/", DATASET="news_29_80k")
