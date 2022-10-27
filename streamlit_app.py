import secrets
import streamlit as st
import pyarrow as pa
import pandas as pd
import altair as alt
import snowflake.connector
import sys
import os
import json
import numpy as np
from datetime import datetime
import datetime as dt
import pytz
#  map chart
import pydeck as pdk

# for data frame tables display
from st_aggrid import AgGrid as stwrite
from st_aggrid.grid_options_builder import GridOptionsBuilder
 
# Initialize connection, using st.experimental_singleton to only run once.
@st.experimental_singleton
def init_connection():
    con = snowflake.connector.connect(
        # user=os.getenv("USER"),
        # password=os.getenv("PASSWORD"),
        # account=os.getenv("ACCOUNT"),
        # role=os.getenv("ROLE"),
        # warehouse=os.getenv("WAREHOUSE"),
        # **st.secrets["USER"],
        # **st.secrets["PASSWORD"],
        # **st.secrets["ACCOUNT"],
        # **st.secrets["ROLE"],
        # **st.secrets["WAREHOUSE"],
        **st.secrets["snowflake"], client_session_keep_alive=True
    )
    return con

st.set_page_config(layout="wide")

def exec_sql(sess, query):
    try:
        df=pd.read_sql(query,sess)
    except:
        st.error("Oops! "+ query + "error executing " + str(sys.exc_info()[0]) + " occurred.")
    else:
        return df
    return

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
    global conn

    ipAddress = ''
    # query_txt = "SELECT b.country, b.lat lat, b.lng long, count(event_id) from login_history_vw a JOIN ipinfo_share_demo.public.location b ON ipinfo_share_demo.public.TO_JOIN_KEY(a.client_ip) = b.join_key AND ipinfo_share_demo.public.TO_INT(a.client_ip) BETWEEN b.start_ip_int AND b.end_ip_int WHERE NOT (a.user_name='WORKSHEETS_APP_USER') GROUP BY b.country, city_region, b.lat, b.lng;"
    query_txt = "SELECT a.client_ip as IP, concat(b.city, ', ', b.region) as city_region, b.country, b.lat, b.lng, count(event_id) from snowflake.account_usage.login_history a JOIN IP_LOCATION_MAPPING b ON b.CLIENT_IP = a.client_ip GROUP BY b.country, city_region, b.lat, b.lng, IP;"

    # st.write("Output for ", query_txt)
    qdf=exec_sql(conn, query_txt)

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
        zoom_level = 2.5 # roughly european country size

        theMap = map(displayQdf, midpoint[0], midpoint[1], zoom_level)

    with tabIPData:

        # st.map(qdf, 40.7090, -76.5, zoomLevel)
        gb = GridOptionsBuilder.from_dataframe(qdf)
        gb.configure_pagination()
        gridOptions = gb.build()

        stwrite(qdf, gridOptions=gridOptions)
        # stwrite(qdf)

tab1, tab2, tab3, tab4 = st.tabs(["Recent Users", "Top 25 Users", "Attrition", "Map"])

# st.title("📃 Dunder Mifflin")
conn = init_connection()
cur = conn.cursor()

with tab1:
    st.header("Recent Users")

    cur.execute("SELECT * FROM VW_RECENTUSERS WHERE USER_NAME != 'SYSTEM'")
    results = cur.fetch_pandas_all()
    #results = results.set_index('USER_NAME')

    c = alt.Chart(results).mark_circle().encode(
        x='QUERYCOUNT', y='TOTALUSAGE', size='USER_NAME', color='USER_NAME', tooltip=['QUERYCOUNT', 'TOTALUSAGE', 'USER_NAME'])
    ## user_name = results.astype(str)

    # st.bar_chart(results)

    st.altair_chart(c, use_container_width=True)

with tab2:
    st.header("Top 25 Users")

    cur.execute("SELECT USER_NAME, TOTALUSAGE FROM VW_RECENTUSERS WHERE TOTALUSAGE > 0 AND USER_NAME != 'SYSTEM'")
    results2 = cur.fetch_pandas_all()
    # results2 = results2.set_index('USER_NAME')

    convert_dict = {'USER_NAME': str, 'TOTALUSAGE': int}
    results2 = results2.astype(convert_dict)

    results2 = results2.sort_values(by='TOTALUSAGE', ascending=False)
    results2 = results2.head(25)

    st.bar_chart(results2, x='USER_NAME', y='TOTALUSAGE')

with tab3:
    st.header("Attrition")

    cur.execute("select USER_NAME, QUERYCOUNT, MAX_QUERY_DATE from vw_userattrition where user_name <> 'SYSTEM' and QUERYCOUNT > 1 order by QUERYCOUNT desc limit 25")
    results3 = cur.fetch_pandas_all()

    convert_dict2 = {'USER_NAME': str, 'QUERYCOUNT': int, 'MAX_QUERY_DATE': str}
    results3 = results3.astype(convert_dict2)

    gb2 = GridOptionsBuilder.from_dataframe(results3)
    gb2.configure_pagination()
    gridOptions2 = gb2.build()
    
    stwrite(results3, gridOptions=gridOptions2)

with tab4:
    show_map()


# with st.form("step-1-form"):
#     first_name = st.text_input("Your first name", placeholder="Dwight")
#     last_name = st.text_input("Your last name", placeholder="Schrute")
#     department = st.selectbox('Which department do you work in?',('Sales','Accounting','HR'))
#     job_title = st.text_input("Your job title", placeholder="Assistant to the regional manager")
#     step_1_submit = st.form_submit_button("Next")

# if step_1_submit and not last_name and first_name:
#     st.warning('Please enter your last name.')
# elif step_1_submit and last_name and not first_name:
#     st.warning('Please enter your first name.')

# if step_1_submit and first_name and last_name:
#     st.session_state['first_name'] = first_name
#     st.session_state['last_name'] = last_name
#     st.session_state['job_title'] = job_title
#     st.session_state['department'] = department

# if 'first_name' not in st.session_state and 'last_name' not in st.session_state:
#     st.write(" ")

# elif 'first_name' in st.session_state and 'last_name'  in st.session_state:
#     st.subheader('Please rate the following statements on a scale from 1 to 5, with 1 being "strongly disagree" and 5 being "strongly agree."')

#     with st.form("step-2-form"):
#         satisfaction_wlb = st.slider('I have great work-life balance at Dunder Mifflin.', 1, 10, key="satisfaction-wlb")
#         satisfaction_culture = st.slider("I enjoy Dunder Mifflin's company culture.", 1, 10, key="satisfaction-culture")
        
#         if department=="Sales":
#             satisfaction_mgr = st.slider('My manager, Michael Scott, is effective.', 1, 10, key="satisfaction-mgr")
#         elif department=="Accounting":
#             satisfaction_mgr = st.slider('My manager, Oscar Nunez, is effective.', 1, 10, key="satisfaction-mgr")
#         elif department=="HR":
#             satisfaction_mgr = st.slider('My manager, Toby Flenderson, is effective.', 1, 10, key="satisfaction-mgr")
#         satisfaction_events = st.slider('I enjoy company events, such as the Dundies and Crime Aid.', 1, 10, key="satisfaction-events")
#         satisfaction_office = st.slider('The facilities are clean and functional.', 1, 10, key="satisfaction-office")
#         step_2_submit = st.form_submit_button("Submit")

#     if step_2_submit:
#         conn = init_connection()
#         first_name = st.session_state['first_name']
#         last_name = st.session_state['last_name']
#         cur = conn.cursor()
#         cur.execute('use EMPLOYEE_SURVEY.PUBLIC')
#         cur.execute("INSERT INTO RESPONSES (FIRST_NAME, LAST_NAME, JOB_TITLE, DEPARTMENT, SATISFACTION_WLB, SATISFACTION_CULTURE, SATISFACTION_MGR, SATISFACTION_EVENTS, SATISFACTION_OFFICE) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (first_name, last_name, st.session_state['job_title'], st.session_state['department'], satisfaction_wlb, satisfaction_culture, satisfaction_mgr, satisfaction_events, satisfaction_office))
#         cur.execute("SELECT * FROM RESPONSES")
#         results = cur.fetchall()

#         st.balloons()
#         st.success("Thanks for submitting your response!")

#         cur.execute('select avg(SATISFACTION_WLB) from RESPONSES')
#         avg_satisfaction_wlb = cur.fetchone()
#         avg_satisfaction_wlb = str(round(avg_satisfaction_wlb[0],3)) + " out of 10"

#         cur.execute('select avg(SATISFACTION_CULTURE) from RESPONSES')
#         avg_satisfaction_culture = cur.fetchone()
#         avg_satisfaction_culture = str(round(avg_satisfaction_culture[0],3)) + " out of 10"

#         cur.execute('select avg(SATISFACTION_MGR) from RESPONSES')
#         avg_satisfaction_mgr = cur.fetchone()
#         avg_satisfaction_mgr = str(round(avg_satisfaction_mgr[0],3)) + " out of 10"

#         cur.execute('select avg(SATISFACTION_EVENTS) from RESPONSES')
#         avg_satisfaction_events = cur.fetchone()
#         avg_satisfaction_events = str(round(avg_satisfaction_events[0],3)) + " out of 10"

#         cur.execute('select avg(SATISFACTION_OFFICE) from RESPONSES')
#         avg_satisfaction_office = cur.fetchone()
#         avg_satisfaction_office = str(round(avg_satisfaction_office[0],3)) + " out of 10"

#         st.metric("Average satisfaction with work-life balance", avg_satisfaction_wlb)
#         st.metric("Average satisfaction with company culture", avg_satisfaction_culture)
#         st.metric("Average satisfaction with manager", avg_satisfaction_mgr)
#         st.metric("Average satisfaction with events", avg_satisfaction_events)
#         st.metric("Average satisfaction with office", avg_satisfaction_office)

#         for key in st.session_state.keys():
#             del st.session_state[key]
