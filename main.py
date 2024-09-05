import pandas as pd
import requests 
from bs4 import BeautifulSoup

i = 0
header_names = []
rows = []
while i < 1:
    i += 1
    url = f'https://tanba.kezekte.kz/ru/frameless/animal/list?p={i}'

    response = requests.get(url, verify=False)
    soup = BeautifulSoup(response.content, 'html.parser')

    # GET HEADER NAME -----

    if i == 1:
        for th in soup.find_all('th'):
            header_names.append(th.text.strip())

    # GET DATA -----

    # for tr in soup.find_all('tr')[1:]:
    #     cells = []
    #     tds = tr.find_all('td')
    #     for td in tds:
    #         cells.append(td.text.strip())
    #     rows.append(cells)

    for tr in soup.find_all('tr')[1:]:
        cells = []
        tds = tr.find_all('td')
        for td in tds:
            cells.append(td.text.strip())
        rows.append(cells)
        # animal_id = tds[0].text.strip()
        # animal_type = tds[1].text.strip()
        # animal_nickname = tds[2].text.strip()
        # animal_gender = tds[3].text.strip()
        # animal_breed = tds[4].text.strip()
        # animal_passport = tds[5].text.strip()
        # animal_register = tds[6].text.strip()
        # animal_birthday = tds[7].text.strip()
        # anima_status = tds[8].text.strip()
        # animal_ownership = tds[9].text.strip()
        # animal_register_type = tds[10].text.strip()
        # animal_tagging_location = tds[11].text.strip()
        # animal_tagging_place = tds[12].text.strip()
        # animal_tagging_date = tds[13].text.strip()
        # animal_district_area = tds[14].text.strip()
        # animal_date_vaccination = tds[15].text.strip()
        # animal_date_sterilization_castration = tds[16].text.strip()

        # cells.append(animal_id,
        #       animal_type,
        #       animal_nickname,
        #       animal_gender,
        #       animal_breed,
        #       animal_passport,
        #       animal_register,
        #       animal_birthday,
        #       anima_status,
        #       animal_ownership,
        #       animal_register_type,
        #       animal_tagging_location,
        #       animal_tagging_place,
        #       animal_tagging_date,
        #       animal_district_area,
        #       animal_date_vaccination,
        #       animal_date_sterilization_castration)
        # for td in tds:
        #     animals_id = td[0]
        #     print(animals_id)
        print('---')
print(rows)
# print(rows[1][0])
# for row in rows:
#     row_id = row[0]
#     print(type(int(row_id)))

# df = pd.DataFrame(rows, columns=header_names)
# print(df)
