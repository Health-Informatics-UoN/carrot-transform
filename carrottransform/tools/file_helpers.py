import os
import sys
import json

# Function inherited from the "old" CaRROT-CDM (modfied to exit on error)
    
def load_json(f_in):
    try:
        data = json.load(open(f_in))
    except Exception as err:
        print ("{0} not found. Or cannot parse as json".format(f_in))
        sys.exit()

    return data

