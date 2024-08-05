#!/usr/bin/env python3
from skyfield import almanac, api
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import mgrs
import plotly.graph_objects as go
import kaleido

def sun(df):
    """
    Calculates the sunset and sunrise (local time) for a specific location (lat, lon) and a specific day.
    Creates a dichotomous variable called daylight with 1 if the event occurred at daylight.
    :param df: Dataframe that contains lat, lon and Datetime column.
    :return:
    sunset with format %Y-%m-%dT%H:%M:%S
    sunrise with format %Y-%m-%dT%H:%M:%S
    daylight dichotomous variable.
    """
    try:
        # Ephemeris bsp file to calculate the position of the sun at a specific location and date.
        eph = api.load('de421.bsp')
        ts = api.load.timescale()
        # GeographicPosition object
        location = api.wgs84.latlon(df['lat'], df['lon'])
        # Two UTC timescale object to find sunsets sunrise in between
        t0 = ts.utc(df['Datetime'].year, df['Datetime'].month, df['Datetime'].day, 0)
        t1 = ts.utc(df['Datetime'].year, df['Datetime'].month, df['Datetime'].day, 23)
        # Find all sunsets and sunrises at location between t0 and t1
        t, y = almanac.find_discrete(t0, t1, almanac.sunrise_sunset(eph, location))
        # Sunrise and sunset with UTC +3 (Iraq Offset)
        sunrise = datetime.strptime(t.utc_iso()[0], '%Y-%m-%dT%H:%M:%SZ') + timedelta(hours=3)
        sunset = datetime.strptime(t.utc_iso()[1], '%Y-%m-%dT%H:%M:%SZ') + timedelta(hours=3)
        # Conditional if the event is at daylight ot not
        if sunrise.time() < df['Datetime'].time() < sunset.time():
            daylight = 1
            return sunrise, sunset, daylight
        else:
            daylight = 0
            return sunrise, sunset, daylight
    except Exception:
        return np.nan, np.nan, np.nan


def mgrs_to_latlon(x):
    """
    Converts MGRS coordinates to lat, lon format
    :param x: MGRS coordinates
    :return: Lat, Lon coordinates
    """
    try:
        # Initialize MGRS object
        m = mgrs.MGRS()
        latlon = m.toLatLon(x)
        lat, lon = latlon
        lat = str(lat).replace("(","")
        lon = str(lon).replace(")","")
        return float(lat), float(lon)
    except Exception:
        return np.nan, np.nan


def dataframe_format(iraq_dataset):
    """
    Data preprocessing. Filters the iraq_sigacts dataset by category (Indirect fire, IED and Safire) and by type of unit
    (CF, Coalition Forces and ISF).
    Converts Datetime column to datetime object with '%Y-%m-%d %H:%M' format and maps type of unit column to contain
    only Coalition forces and Iraqi Security Forces.
    Capitalizes Type, Category and Affiliation columns.
    :param iraq_dataset: iraq_sigacts dataset
    :return: Dataframe
    """
    # Read dsv data
    df = pd.read_csv(iraq_dataset).dropna()
    # Filter by IDF, IED and Safire
    df = df[df['Category'].str.contains('Indirect Fire|IED Explosion|Safire')]
    # Filter by CF and ISF
    df = df[df['Type_of_unit'].str.contains('CF|Coalition|Coalition Forces|ISF')]
    # Datetime format
    df['Datetime'] = pd.to_datetime(df['Datetime'])
    # Type of unit name normalization
    type_of_unit = {'CF': 'Coalition Forces',
                    'Coalition': 'Coalition Forces',
                    'ISF': 'Iraqi Security Forces'}
    df[df.columns] = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df["Type_of_unit"] = df['Type_of_unit'].map(type_of_unit).fillna(df['Type_of_unit'])
    # Capitalize string columns
    df[['Type', 'Category', 'Affiliation']] = df[['Type',
                                                  'Category',
                                                  'Affiliation']].apply(lambda x: x.str.capitalize())
    return df

def create_bar_chart(dask_time, pandas_time):
  # Crear un gráfico de barras comparando el tiempo de Dask y Pandas
  labels = ['Dask', 'Pandas']
  times = [dask_time, pandas_time]

  fig = go.Figure(data=[go.Bar(
      x=labels,
      y=times,
      text=[f'{dask_time:.2f} s', f'{pandas_time:.2f} s'],
      textposition='auto',
      marker_color=['blue', 'orange'],
      width=[0.5, 0.5]
  )])



  fig.update_layout(
      title='Comparison of execution time: Dask vs Pandas (CPU Intel Core i7-10710U)',
      xaxis_title='Library',
      yaxis_title='Time (seconds)',
      yaxis=dict(range=[0, max(times) + 50]),
      bargap=0.1,
      autosize=False,
      width=800,
      height=500

  )

  fig.show()
  fig.write_image('dask_pandas_comparison.jpg')