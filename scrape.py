""" Scrapes Hamilton County 911 site for specific incident data into a dataframe. """

import pandas as pd
import requests
import urllib.request
import time

from bs4 import BeautifulSoup


def stack_dfs(df1, df2):
    """ Appends df2 to the end of df1, but assigns df2 to df1 if df1 is None """
    
    if df1 is None:
        df1 = df2
    else:
        df1 = df1.append(df2, ignore_index=True) # TODO: may need to get rid of this if acutally checking indices for duplicates
    return df1


def get_data():
    """ Returns the table of incident rows matching the listed criteria. """
    
    # get the html
    url = "https://www.hc911.org/responses.php"
    response = requests.get(url)

    # turn into BS4 object
    soup = BeautifulSoup(response.text, "html.parser")

    data = []

    # parse for the table and rows of the incidents table
    tag = soup.find(id='incidents')
    rows = tag.findAll("tr")
    
    firstRowSkipped = False # first row flag
    for row in rows:
        # skip the first row because it's just header stuff
        if not firstRowSkipped:
            firstRowSkipped = True
            continue

        # get all of the cells of that row
        tds = row.findAll("td")

        # get the data dictionary for this row
        row_data = {
                "Master Incident" : tds[0].text,
                "Created" : tds[1].text,
                "Status" : tds[2].text,
                "Agency" : tds[3].text,
                "Type" : tds[4].text,
                "Location" : tds[5].text,
                "Cross Streets" : tds[6].text
                }
        data.append(row_data)

    # create dataframe
    df = pd.DataFrame(data)

    # filter out certain types
    blacklist_types = ["EMS CALL", "Well Being Check"]
    masked = df.Type.isin(blacklist_types)
    filtered_df = df[~masked]
    
    # filter to only include certain locations
    whitelist_strs = ["Brainerd Rd", "Douglas"]
    
    building_df = None
    # loop through the whitelisted location
    for filter_str in whitelist_strs:
        # only get the rows with this location in it
        matching_df = filtered_df[filtered_df.Location.str.contains(filter_str)]
        building_df = stack_dfs(building_df, matching_df)
    loc_df = building_df
    
    return loc_df


if __name__ == "__main__":
    
    while(True):
        print("Scraping...")
        df = get_data()
        print(df)
        
        time.sleep(60)
