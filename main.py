"""Export data from WinCC Unified SQlite database."""

import datetime
import os
import sqlite3
from dataclasses import dataclass

import numpy as np
import pandas as pd
import plotly.express as px

IMAGE_SIZE = {
    'width': 1600,
    'height': 800,
}

TS_FORMAT = '%Y-%m-%d %H:%M'
TS_FORMAT_FILE = '%Y-%m-%d %H-%M'

SQL = """
SELECT Value, pk_TimeStamp
FROM LoggedProcessValue
WHERE pk_fk_Id = {tagid}
"""


def convert_ldap(timestamp: int) -> datetime:
    """Convert LDAP/AD timestamp to python datetime."""
    if timestamp != 0:
        return datetime.datetime(1601, 1, 1) + datetime.timedelta(
            seconds=timestamp / 10000000,
        )
    return np.nan


@dataclass
class LoggedTag:
    """Class for logged tag."""

    tagid: int
    name: str
    title_y: str


if __name__ == '__main__':
    chunks = [
        'OS_TLG5_20210915_070502.db3',
        'OS_TLG5_20210922_070502.db3',
        'OS_TLG5_20210929_070502.db3',
    ]

    tags = [
        LoggedTag(
            tagid=1895,
            name='1H01A-TT02',
            title_y='Температура, [℃]',
        ),
        LoggedTag(
            tagid=1893,
            name='1H01A-TT03',
            title_y='Температура, [℃]',
        ),
        LoggedTag(
            tagid=1918,
            name='1H01A-WC01',
            title_y='Вес, [кг]',
        ),
        LoggedTag(
            tagid=1901,
            name='1H01A-PT01',
            title_y='Давление, [мбар]',
        ),
        LoggedTag(
            tagid=1899,
            name='1H01A-PT02',
            title_y='Давление, [бар]',
        ),
    ]

    for tag in tags:
        df_chunks = []

        for chunk in chunks:
            df_chunks.append(
                pd.read_sql_query(
                    sql=SQL.format(
                        tagid=tag.tagid,
                    ),
                    con=sqlite3.connect(
                        database=f'/home/konstantin/temp/{chunk}',
                    ),
                ),
            )

        df = pd.concat(df_chunks)

        df['pk_TimeStamp'] = df['pk_TimeStamp'].fillna(0).apply(convert_ldap)

        df = df.sort_values(by='pk_TimeStamp')

        start = df['pk_TimeStamp'].min()
        stop = df['pk_TimeStamp'].max()

        idx = pd.period_range(
            start=pd.to_datetime(start).floor(freq='8H'),
            end=pd.to_datetime(stop).ceil(freq='8H'),
            freq='8H',
        )

        output_folder = f'output/{tag.name}'
        os.makedirs(output_folder, exist_ok=True)

        for i in range(len(idx) - 1):
            start = idx[i].to_timestamp()
            stop = idx[i + 1].to_timestamp()

            df_temp = df[
                (df['pk_TimeStamp'] >= start)
                & (df['pk_TimeStamp'] <= stop)]
            px.line(
                data_frame=df_temp,
                x='pk_TimeStamp',
                y='Value',
                title=(f'{tag.name}, '
                       f'{start.strftime(TS_FORMAT)} - '
                       f'{stop.strftime(TS_FORMAT)}'),
                labels={
                    'Value': tag.title_y,
                    'pk_TimeStamp': 'Время',
                },
            ).write_image(
                (
                    f'{output_folder}/{tag.name}, '
                    f'{start.strftime(TS_FORMAT_FILE)} - '
                    f'{stop.strftime(TS_FORMAT_FILE)}.png'
                ),
                **IMAGE_SIZE,
            )
