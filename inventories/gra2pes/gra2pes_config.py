class Gra2pesConfig():
    month_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    sector_details = {
        'AG' : {'description': 'Agriculture', 'years':[2021], 'months':month_list},
        'AVIATION' : {'description': 'Aviation', 'years':[2021], 'months':month_list},
        'COMM' : {'description': 'Commercial', 'years':[2021], 'months':month_list},
        'COOKING' : {'description': 'Cooking', 'years':[2021], 'months':month_list},
        'EGU' : {'description': 'Electricity Generation', 'years':[2021], 'months':month_list},
        'FUG' : {'description': 'Fugitive', 'years':[2021], 'months':month_list},
        'INDF' : {'description': 'Industrial fuel', 'years':[2021], 'months':month_list},
        'INDP' : {'description': 'Industrial processes', 'years':[2021], 'months':month_list},
        'INTERNATIONAL' : {'description': 'International', 'years':[2021], 'months':month_list},
        'OFFROAD' : {'description': 'Off-road vehicles', 'years':[2021], 'months':month_list},
        'OG' : {'description': 'Oil and gas', 'years':[2021], 'months':month_list},
        'ONROAD_DSL' : {'description': 'On-road diesel', 'years':[2021], 'months':month_list},
        'ONROAD_GAS' : {'description': 'On-road gasoline', 'years':[2021], 'months':month_list},
        'RAIL' : {'description': 'Rail', 'years':[2021], 'months':month_list},
        'RES' : {'description': 'Residential', 'years':[2021], 'months':month_list},
        'SHIPPING' : {'description': 'Shipping', 'years':[2021], 'months':month_list},
        'VCP' : {'description': 'VCP', 'years':[2021], 'months':month_list},
        'WASTE' : {'description': 'Waste', 'years':[2021], 'months':month_list},
    }

    def __init__(self):
        pass