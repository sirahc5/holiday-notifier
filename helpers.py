import os
import json
import pandas as pd
import requests
import smtplib
import ssl
import datetime


def get_data(token: str, country: str, year: str, month: str, day: str) -> \
requests.models.Response:
    r = requests.get(f'https://calendarific.com/api/v2/holidays?api_key='
                     f'{token}&country={country}&year={year}&month={month}&day='
                     f'{day}')

    return r


def json_to_df(data: str) -> pd.DataFrame:
    """
    Creates a pandas DataFrame from the json data
    Args:
        data: the json data
    Returns:
        DataFrame: pandas DataFrame with columns: name, description,
        country, date, holiday_type
    """

    name = []
    description = []
    country = []
    date = []
    holiday_type = []

    for holiday in data["response"]["holidays"]:
        name.append(holiday["name"])
        description.append(holiday["description"])
        country.append(holiday["country"]["name"])
        date.append(holiday["date"]["iso"])
        holiday_type.append(holiday["type"][0])

    holiday_dict = {
        "name": name,
        "description": description,
        "country": country,
        "date": date,
        "holiday_type": holiday_type
    }

    holidays_df = pd.DataFrame(holiday_dict,
                              columns=[
                                  'name', 'description', 'country', 'date',
                                  'holiday_type'
                                   ]
                            )
    return holidays_df


def check_data_validity(df: pd.DataFrame, primary_key: str) -> bool:
    '''
    Checks whether the DataFrame is empty, whether the primary key is unique and
    whether any null values exist
    Args:
        df: pandas DataFrame
        primary_key: string
    Returns:
        True/False
    '''

    # Check if the dataframe is empty:
    if df.empty:
        print('There are no official holidays today.')
        return False

    # Check primary key:
    if pd.Series(df[primary_key]).is_unique:
        pass
    else:
        raise Exception('Primary key check failed.')

    # Check for nulls:
    if df.isnull().values.any():
        raise Exception('Null values found.')

    return True

    # # Check that all holidays that are returned are of today's date:
    # returned_dates = df['date'].tolist()
    # for returned_date in returned_dates:
    #     if datetime.date.today().strftime('%Y-%m-%d') != returned_date:
    #         raise Exception('''At least one of the holidays returned is not
    #         celebrated today''')
    #
    # return True


def send_email(sender_email: str, receiver_email: str, message: str) -> None:
    '''
    Sends email using the smtplib library
    Args:
        sender_email: email address of the service account used
        receiver_email: email address that will receive the holiday
                        notifications
        message: email message to be sent
    Returns:
        True/False
    '''
    # Starting a secure a secure SMTP connection:
    port = 465
    password = os.environ.get('SENDERPASS')

    # Creating a secure SSL context:
    context = ssl.create_default_context()

    # Closing the connection at the end of the indented code block:
    with smtplib.SMTP_SSL('smtp.gmail.com', port, context=context) as server:
        # server.ehlo()
        # server.starttls()
        # server.ehlo()
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message)
