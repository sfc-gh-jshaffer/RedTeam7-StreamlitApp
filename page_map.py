#!/usr/bin/env python3

import sys
import os
import streamlit as st
import json
import pandas as pd
import numpy as np
import snowflake.connector
from datetime import datetime
import datetime as dt
import pytz
#  map chart
import pydeck as pdk

# for data frame tables display
from st_aggrid import AgGrid as stwrite
from st_aggrid.grid_options_builder import GridOptionsBuilder

# for role chart
import graphviz as graphviz
#annoated text
from annotated_text import annotated_text as atext

radiolist = {
    "Map": "show_map"
}


def write_env(sess):
    df=exec_sql(sess,"select current_region() region, current_account() account, current_user() user, current_role() role, current_warehouse() warehouse, current_database() database, current_schema() schema ")
    df.fillna("N/A",inplace=True)
    csp=df.at[0,"REGION"]
    cspcolor="#ff9f36"
    if "AWS"  in csp :
        cspcolor="#FF9900"
    elif "AZURE" in csp:
        cspcolor = "#007FFF"
    elif "GCP" in csp:
        cspcolor = "#4285F4"
    atext((csp,"REGION",cspcolor)," ",
          (df.at[0,"ACCOUNT"],"ACCOUNT","#2cb5e8")," ",
          (df.at[0,"USER"],"USER","#afa" ),          " ",
          (df.at[0,"ROLE"],"ROLE", "#fea"),       " ",
          (df.at[0,"WAREHOUSE"],"WAREHOUSE","#8ef"),     " ",
          (df.at[0,"DATABASE"],"DATABASE"),           " ",
          (df.at[0,"SCHEMA"],"SCHEMA"),
          )

def exec_sql(sess, query):
    try:
        df=pd.read_sql(query,sess)
    except:
        st.error("Oops! "+ query + "error executing " + str(sys.exc_info()[0]) + " occurred.")
    else:
        return df
    return




def create_session():
    with open('creds-tko2.json') as f:
        cp = json.load(f)

    conn = snowflake.connector.connect(
                    user=cp["user"],
                    password=cp["password"],
                    account=cp["account"],
                    warehouse=cp["warehouse"],
                    database=cp["database"],
                    role=cp["role"],
                    schema=cp["schema"]
                    )

    return conn


curr_sess = create_session()

# CALCULATE MIDPOINT FOR GIVEN SET OF DATA
@st.experimental_memo
def mpoint(lat, lon):
    return (np.average(lat), np.average(lon))

def map(data, lat, lon, zoom):
    st.write(
        pdk.Deck(
            map_style="mapbox://styles/mapbox/light-v9",
            initial_view_state={
                "latitude": lat,
                "longitude": lon,
                "zoom": zoom,
                "pitch": 0,
            },
            layers=[
                pdk.Layer(
                    "HeatmapLayer",
                    data=data,
                    get_position=["lon", "lat"],
                    auto_highlight=True,
                    get_radius=1000,
                    get_fill_color='[180, 0, 200, 140]',
                    pickable=True
                ),
            ],
        )
    )

def show_map():
    global curr_sess

    ipAddress = ''
    # query_txt = "SELECT b.country, b.lat lat, b.lng long, count(event_id) from login_history_vw a JOIN ipinfo_share_demo.public.location b ON ipinfo_share_demo.public.TO_JOIN_KEY(a.client_ip) = b.join_key AND ipinfo_share_demo.public.TO_INT(a.client_ip) BETWEEN b.start_ip_int AND b.end_ip_int WHERE NOT (a.user_name='WORKSHEETS_APP_USER') GROUP BY b.country, city_region, b.lat, b.lng;"
    query_txt = "SELECT a.client_ip as IP, concat(b.city, ', ', b.region) as city_region, b.country, b.lat, b.lng, count(event_id) from snowflake.account_usage.login_history a JOIN IP_LOCATION_MAPPING b ON b.CLIENT_IP = a.client_ip GROUP BY b.country, city_region, b.lat, b.lng, IP;"

    # st.write("Output for ", query_txt)
    qdf=exec_sql(curr_sess, query_txt)

    qdf.columns = ['IP', 'city','country','lat','lon','numLogins']

    # qdf['numLogins'] = qdf['numLogins'].apply(lambda x:  x*10000)
    # st.write(qdf)

    tabMap, tabIPData = st.tabs(["Map", "IP Data"])

    with tabMap:

        # # allIPs = st.dataframe([""])
        # allIPs = qdf["IP"].unique()
        # ipBox = st.selectbox("IP", allIPs)

        # qdf.filter(like='71.191.73.12',axis='IP')
        if (ipAddress == '') :
            filterString = ''
            displayQdf = qdf
        else :
            filterString = 'IP == "' + str(ipAddress) + '"'
            displayQdf = qdf.query(filterString)

        # st.write(TypeOf(allIPs))
        midpoint = mpoint(displayQdf["lat"], displayQdf["lon"])
        zoom_level = 5 # roughly european country size

        theMap = map(displayQdf, midpoint[0], midpoint[1], zoom_level)

    with tabIPData:

        # st.map(qdf, 40.7090, -76.5, zoomLevel)
        gb = GridOptionsBuilder.from_dataframe(qdf)
        gb.configure_pagination()
        gridOptions = gb.build()

        stwrite(qdf, gridOptions=gridOptions)
        # stwrite(qdf)


 
def main():
    global curr_sess
    st.set_page_config(page_title='Awesome Snowflake', layout="wide")
    st.sidebar.title("Navigation")
    selection = st.sidebar.radio("Go to", list(radiolist.keys()))
    selectoption = radiolist[selection]
    with st.sidebar:
        write_env(curr_sess)
    possibles = globals().copy()
    possibles.update(locals())
    method = possibles.get(selectoption)
    if not method:
        raise NotImplementedError("Method %s not implemented" % method)
    method()
    st.sidebar.title("Documentation")
    st.sidebar.info(
        "Powered by Snowflake/Streamlit"
        "Here is documentations for [Snowflake](https://docs.snowflake.com/en/index.html) and [Streamlit](https://docs.streamlit.io/)"
        " Use this guide for setup [Snowflake Quickstarts](https://quickstarts.snowflake.com/guide/getting_started_with_snowflake/index.html?index=..%2F..index#0)"
    )

if __name__ == "__main__":
    main()
    # st.snow()
