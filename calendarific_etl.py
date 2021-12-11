import os
import pandas as pd
import json
import sqlite3
import datetime
import sqlalchemy
import helpers
import smtplib
import re
from sqlalchemy.orm import sessionmaker # ORM


def run_calendarific_etl():
    db_location = 'sqlite:///holidays.sqlite'
    user_id = 'Charis'
    token = os.environ.get('CALENDARIFICTOKEN')

    # Extract part of the ETL process:
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorisation': f'Bearer {token}'
    }

    country = 'uk'
    # Using tomorrow's date because airflow scheduler runs jobs at the end of
    # the day:
    # year = datetime.date.today().strftime('%Y')
    year = str(int(datetime.date.today().strftime('%Y')) + 1)
    # day = datetime.date.today().strftime('%d')
    day = str(int(datetime.date.today().strftime('%d')) + 1)
    # month = datetime.date.today().strftime('%m')
    month = str(int(datetime.date.today().strftime('%m')) + 1)
    today = datetime.date.today().strftime('%Y-%m-%d')
    tomorrow = (today + datetime.timedelta(days=1)).strftime('%Y-%m-%d')

    r = helpers.get_data(token, country, year, month, day)
    data = r.json()

    holidays_df = helpers.json_to_df(data)
    print(holidays_df)

    # Transform:
    if helpers.check_data_validity(holidays_df, 'description'):
        print('The data are valid.')
    else:
        raise Exception('The dataframe is empty.')

    # Load:
    engine = sqlalchemy.create_engine(db_location)
    conn = sqlite3.connect('holidays.sqlite')
    cursor = conn.cursor()
    sql_query = '''
    CREATE TABLE IF NOT EXISTS holidays(
        name VARCHAR(200),
        description VARCHAR(255),
        country VARCHAR(56),
        date CHAR(10),
        holiday_type VARCHAR(200)
    )
    '''
    cursor.execute(sql_query)
    print('Opened database successfully.')
    try:
        holidays_df.to_sql('holidays', engine, index=False, if_exists='append')
    except:
        print('The data is already in the database.')

    # Send e-mail:
    results = cursor.execute(f'''SELECT * FROM holidays
                                 WHERE date = (?)''', (tomorrow,))
    rows = results.fetchall()
    print(f'results: {rows}')
    sender = os.environ.get('SENDEREMAIL')
    receiver = os.environ.get('MYEMAIL')
    user = re.findall(r'(.+)@', receiver)[0]
    # If the call is successful, the status code will be 200 and the code will
    # run and send the email:
    if r.status_code == 200:
    # if helpers.check_data_validity(holidays_df, 'description'):
        try:
            # If the info from the API call is not there, i.e. there is no
            # holiday on that day, an IndexError will be raised and no message
            # will be sent.

            # Assigning query results to f-string variables:
            name = rows[0][0]
            description = rows[0][1]
            country = rows[0][2]

            # Composing the message:
            subject = 'Your Daily Holiday Notification'
            body = (f'Hello, {user}. This is your holiday notification. '
                   f'Today it is a holiday in {country}! It is the {name}. '
                   f'{description}. Have a great day!')
            msg = f'Subject: {subject}\n\n{body}'

            # Sending the message as email:
            helpers.send_email(sender, receiver, msg)

        except IndexError:
            # If the website does not provide any holidays corresponding to the
            # day, then no email will be sent to the user:
            pass
    # If the call is unsuccessful, the program prints the error:
    else:
        print(f'{response.status_code}: {response.reason}')

    # Close database:
    conn.close()
    print('Closed database successfully.')
