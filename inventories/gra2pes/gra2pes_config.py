class Gra2pesConfig():
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    years = [2021]
    day_types = ['satdy','sundy','weekdy']
    sector_details = {
        'AG' : {'description': 'Agriculture'},
        'AVIATION' : {'description': 'Aviation'},
        'COMM' : {'description': 'Commercial'},
        'COOKING' : {'description': 'Cooking'},
        'EGU' : {'description': 'Electricity Generation'},
        'FUG' : {'description': 'Fugitive'},
        'INDF' : {'description': 'Industrial fuel'},
        'INDP' : {'description': 'Industrial processes'},
        'INTERNATIONAL' : {'description': 'International'},
        'OFFROAD' : {'description': 'Off-road vehicles'},
        'OG' : {'description': 'Oil and gas'},
        'ONROAD_DSL' : {'description': 'On-road diesel'},
        'ONROAD_GAS' : {'description': 'On-road gasoline'},
        'RAIL' : {'description': 'Rail'},
        'RES' : {'description': 'Residential'},
        'SHIPPING' : {'description': 'Shipping'},
        'VCP' : {'description': 'VCP'},
        'WASTE' : {'description': 'Waste'},
    }

    def __init__(self):
        pass