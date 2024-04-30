# helper functions to use in streamlit_app.py

def date_filter_format(date): 
    return "-".join([str(date.year), str(date.month), str(date.day)])
