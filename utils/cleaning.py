'''
Helper functions for cleaning data
'''
import re

def clean_int(val):
    '''
    Helper function to String data into integers
    '''
    if(not val):
        return None
    try:
        # Remove commas/spaces and convert
        cleaned = "".join(filter(str.isdigit, str(val)))
        if(cleaned):
            return int(cleaned)
        else: None
    except: 
        return None
    
def clean_price(price_str):
    '''
    Helper function to turn String price into float price
    '''
    if(not price_str):
        return None
    try:
        price_str_transformed = re.sub(r'[^\d.]', '', str(price_str))
        return float(price_str_transformed) if clean_price else None
    except:
        return None
