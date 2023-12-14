def into_json(name, city, address, logo, website, ypage, rating, reviews):
    """ Шаблон файла OUTPUT.json"""

    data_grabbed = {
        # "ID": org_id,
        "company_name": name,
        "city": city,
        "address": address,
        "company_url": website,
        "company_rating": rating,
        "logo": logo,
        "yandex_url": ypage,
        "reviews": reviews,
    }
    return data_grabbed


# opening_hours_new = []
# days = ['mo', 'tu', 'we', 'th', 'fr', 'sa', 'su']

# Проверка opening_hours на отсутствие одного их рабочих дней
# Создается отдельный список (opening_hours_new) с полученными значениями
# Далее он проверяется на отсутствие того или иного рабочего дня
# На индекс отсутствующего элемента вставляется значение  "   выходной"
# for day in opening_hours:
#     opening_hours_new.append(day[:2].lower())
# for i in days:
#     if i not in opening_hours_new:
#         opening_hours.insert(days.index(i), '   выходной')

# "opening_hours":
# {
#     "mon": opening_hours[0][3:],
#     "tue": opening_hours[1][3:],
#     "wed": opening_hours[2][3:],
#     "thu": opening_hours[3][3:],
#     "fri": opening_hours[4][3:],
#     "sat": opening_hours[5][3:],
#     "sun": opening_hours[6][3:]
# },
# "phone": phone,
# "social": social,
# "goods": goods,
