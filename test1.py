import requests
from bs4 import BeautifulSoup
import pandas as pd
import re


def sec_dataframe(sec_url):
    r = requests.get(sec_url)
    content = r.text
    soup = BeautifulSoup(content, "html.parser")
    right_table = soup.find('table', class_='list')
    col1 = []
    col2 = []
    col3 = []
    rows = right_table.findAll("tr")
    for tr in rows:
        cols = tr.findAll('td')
        for co in cols:
            link = co.findAll('a')
            for li in link:
                col1.append(li.get('href'))
                col3.append(li.getText())

    for data in col1:
        date = re.findall('\d+', data)
        new_date = date[0][4:] + '-' + date[0][:2] + '-' + date[0][2:4]
        col2.append(new_date)

    e_type = []

    for data in col3:
        if 'Exempt' in data:
            e_type.append('Exempt')
        else:
            e_type.append('Non-exempt')

    col1 = list(map(lambda x: 'www.sec.gov' + x, col1))

    df = pd.DataFrame(col1, columns=['File_url'])
    df['Date'] = col2
    df['Type'] = e_type
    csv_location = '/home/saxena/PycharmProjects/test1/Tests/new_data.csv'
    df.to_csv(csv_location)
    return csv_location


def test_sec_dataframe():
    df1 = pd.read_csv('/home/saxena/PycharmProjects/test1/Tests/sample_data.csv')
    df_loc = sec_dataframe("https://www.sec.gov/help/foiadocsinvafoiahtm.html")
    df2 = pd.read_csv(df_loc)
    assert df1.equals(df2)
