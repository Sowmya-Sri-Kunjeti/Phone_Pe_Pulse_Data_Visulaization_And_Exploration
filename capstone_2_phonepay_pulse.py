import os
import subprocess
import pandas as pd
import mysql.connector
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
import requests
import json
from PIL import Image

### Data extraction through Github cloning


repo_url = 'https://github.com/PhonePe/pulse.git'
local_path = 'C:\\Users\\harsh\\GUVI-Python\\Capstone_2\\Git_files'
cmd_string = 'git clone' + ' ' + repo_url + ' ' + local_path
if not os.path.exists(local_path):
    os.system(cmd_string)

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Root",
    database='phonepay_pulse'
)

### Data Transformation

# Aggregated_Transaction
def get_agg_trans_details():
    path_year_agg_trans = "C:\\Users\\harsh\\GUVI-Python\\Capstone_2\\Git_files\\data\\aggregated\\transaction\\country\\india"
    Agg_trans_year_list = os.listdir(path_year_agg_trans)
    path_state_agg_trans = "C:\\Users\\harsh\\GUVI-Python\\Capstone_2\\Git_files\\data\\aggregated\\transaction\\country\\india\\state\\"
    Agg_trans_state_list = os.listdir(path_state_agg_trans)

    clm1 = {'State':[], 'Year':[],'Quater':[],'Transacion_type':[], 'Transacion_count':[], 'Transacion_amount':[]}

    for i in Agg_trans_state_list:
        p_i = path_state_agg_trans + i + "\\"
        Agg_yr = os.listdir(p_i)
        for j in Agg_yr:
            p_j = p_i + j + "/"
            Agg_yr_list = os.listdir(p_j)
            for k in Agg_yr_list:
                p_k = p_j+k
                Data = open(p_k,'r')
                D = json.load(Data)
                for z in D['data']['transactionData']:
                    Name = z['name']
                    count = z['paymentInstruments'][0]['count']
                    amount = z['paymentInstruments'][0]['amount']
                    clm1['Transacion_type'].append(Name)
                    clm1['Transacion_count'].append(count)
                    clm1['Transacion_amount'].append(amount)
                    clm1['State'].append(i)
                    clm1['Year'].append(j)
                    clm1['Quater'].append(int(k.strip('.json')))
    agg_Trans = pd.DataFrame(clm1)

    agg_Trans = agg_Trans.replace({"andaman-&-nicobar-islands" : "Andaman & Nicobar",
                                   "andhra-pradesh" : "Andhra Pradesh",
                                   "arunachal-pradesh" : "Arunachal Pradesh",
                                   "assam" : "Assam",
                                   "bihar" : "Bihar",
                                   "chandigarh" : "Chandigarh",
                                   "chhattisgarh" : "Chhattisgarh",
                                   "dadra-&-nagar-haveli-&-daman-&-diu" : "Dadra and Nagar Haveli and Daman and Diu",
                                   "delhi" : "Delhi",
                                   "goa" : "Goa",
                                   "gujarat" : "Gujarat",
                                   "haryana" : "Haryana",
                                   "himachal-pradesh" : "Himachal Pradesh",
                                   "jammu-&-kashmir" : "Jammu & Kashmir",
                                   "jharkhand" : "Jharkhand",
                                   "karnataka" : "Karnataka",
                                   "kerala" : "Kerala",
                                   "ladakh" : "Ladakh",
                                   "madhya-pradesh" : "Madhya Pradesh",
                                   "maharashtra" : "Maharashtra",
                                   "manipur" : "Manipur",
                                   "meghalaya" : "Meghalaya",
                                   "mizoram" : "Mizoram",
                                   "nagaland" : "Nagaland",
                                   "odisha" : "Odisha",
                                   "puducherry" : "Puducherry",
                                   "punjab" : "Punjab",
                                   "rajasthan" : "Rajasthan",
                                   "sikkim" : "Sikkim",
                                   "tamil-nadu" : "Tamil Nadu",
                                   "telangana" : "Telangana",
                                   "tripura" : "Tripura",
                                   "uttar-pradesh" : "Uttar Pradesh",
                                   "uttarakhand" : "Uttarakhand",
                                   "west-bengal" : "West Bengal"})

    return agg_Trans

# Map_Transaction
def get_map_trans_details():
    path_year_map_trans = "C:\\Users\\harsh\\GUVI-Python\\Capstone_2\\Git_files\\data\\map\\transaction\\hover\\country\\india"
    Map_trans_year_list = os.listdir(path_year_map_trans)
    path_state_map_trans = "C:\\Users\\harsh\\GUVI-Python\\Capstone_2\\Git_files\\data\\map\\transaction\\hover\\country\\india\\state\\"
    Map_trans_state_list = os.listdir(path_state_map_trans)

    clm2 = {'State':[], 'Year':[],'Quater':[],'Transacion_type':[], 'Transacion_count':[], 'Transacion_amount':[]}

    for i in Map_trans_state_list:
        p_i = path_state_map_trans + i + "\\"
        Agg_yr = os.listdir(p_i)
        for j in Agg_yr:
            p_j = p_i + j + "/"
            Agg_yr_list = os.listdir(p_j)
            for k in Agg_yr_list:
                p_k = p_j+k
                Data = open(p_k,'r')
                D = json.load(Data)
                for z in D['data']['hoverDataList']:
                  Name = z['name']
                  count = z['metric'][0]['count']
                  amount = z['metric'][0]['amount']
                  clm2['Transacion_type'].append(Name)
                  clm2['Transacion_count'].append(count)
                  clm2['Transacion_amount'].append(amount)
                  clm2['State'].append(i)
                  clm2['Year'].append(j)
                  clm2['Quater'].append(int(k.strip('.json')))
    map_trans = pd.DataFrame(clm2)

    map_trans = map_trans.replace({"andaman-&-nicobar-islands" : "Andaman & Nicobar",
                               "andhra-pradesh" : "Andhra Pradesh",
                               "arunachal-pradesh" : "Arunachal Pradesh",
                               "assam" : "Assam",
                               "bihar" : "Bihar",
                               "chandigarh" : "Chandigarh",
                               "chhattisgarh" : "Chhattisgarh",
                               "dadra-&-nagar-haveli-&-daman-&-diu" : "Dadra and Nagar Haveli and Daman and Diu",
                               "delhi" : "Delhi",
                               "goa" : "Goa",
                               "gujarat" : "Gujarat",
                               "haryana" : "Haryana",
                               "himachal-pradesh" : "Himachal Pradesh",
                               "jammu-&-kashmir" : "Jammu & Kashmir",
                               "jharkhand" : "Jharkhand",
                               "karnataka" : "Karnataka",
                               "kerala" : "Kerala",
                               "ladakh" : "Ladakh",
                               "madhya-pradesh" : "Madhya Pradesh",
                               "maharashtra" : "Maharashtra",
                               "manipur" : "Manipur",
                               "meghalaya" : "Meghalaya",
                               "mizoram" : "Mizoram",
                               "nagaland" : "Nagaland",
                               "odisha" : "Odisha",
                               "puducherry" : "Puducherry",
                               "punjab" : "Punjab",
                               "rajasthan" : "Rajasthan",
                               "sikkim" : "Sikkim",
                               "tamil-nadu" : "Tamil Nadu",
                               "telangana" : "Telangana",
                               "tripura" : "Tripura",
                               "uttar-pradesh" : "Uttar Pradesh",
                               "uttarakhand" : "Uttarakhand",
                               "west-bengal" : "West Bengal"})
    return map_trans

# Top_Transaction
def get_top_trans_details():
    path_year_top_trans = "C:\\Users\\harsh\\GUVI-Python\\Capstone_2\\Git_files\\data\\top\\transaction\\country\\india"
    Top_trans_year_list = os.listdir(path_year_top_trans)
    path_state_top_trans = "C:\\Users\\harsh\\GUVI-Python\\Capstone_2\\Git_files\\data\\top\\transaction\\country\\india\\state\\"
    Top_trans_state_list = os.listdir(path_state_top_trans)

    clm3 = {'State':[], 'Year':[],'Quater':[],'Pincodes':[], 'Transaction_count':[], 'Transaction_amount':[]}

    for i in Top_trans_state_list:
        p_i = path_state_top_trans + i + "\\"
        Agg_yr = os.listdir(p_i)
        for j in Agg_yr:
            p_j = p_i + j + "/"
            Agg_yr_list = os.listdir(p_j)
            for k in Agg_yr_list:
                p_k = p_j+k
                Data = open(p_k,'r')
                D = json.load(Data)
                for z in D['data']['pincodes']:
                  Name = z['entityName']
                  count = z['metric']['count']
                  amount = z['metric']['amount']
                  clm3['Pincodes'].append(Name)
                  clm3['Transaction_count'].append(count)
                  clm3['Transaction_amount'].append(amount)
                  clm3['State'].append(i)
                  clm3['Year'].append(j)
                  clm3['Quater'].append(int(k.strip('.json')))
    top_trans = pd.DataFrame(clm3)

    top_trans = top_trans.replace({"andaman-&-nicobar-islands" : "Andaman & Nicobar",
                               "andhra-pradesh" : "Andhra Pradesh",
                               "arunachal-pradesh" : "Arunachal Pradesh",
                               "assam" : "Assam",
                               "bihar" : "Bihar",
                               "chandigarh" : "Chandigarh",
                               "chhattisgarh" : "Chhattisgarh",
                               "dadra-&-nagar-haveli-&-daman-&-diu" : "Dadra and Nagar Haveli and Daman and Diu",
                               "delhi" : "Delhi",
                               "goa" : "Goa",
                               "gujarat" : "Gujarat",
                               "haryana" : "Haryana",
                               "himachal-pradesh" : "Himachal Pradesh",
                               "jammu-&-kashmir" : "Jammu & Kashmir",
                               "jharkhand" : "Jharkhand",
                               "karnataka" : "Karnataka",
                               "kerala" : "Kerala",
                               "ladakh" : "Ladakh",
                               "madhya-pradesh" : "Madhya Pradesh",
                               "maharashtra" : "Maharashtra",
                               "manipur" : "Manipur",
                               "meghalaya" : "Meghalaya",
                               "mizoram" : "Mizoram",
                               "nagaland" : "Nagaland",
                               "odisha" : "Odisha",
                               "puducherry" : "Puducherry",
                               "punjab" : "Punjab",
                               "rajasthan" : "Rajasthan",
                               "sikkim" : "Sikkim",
                               "tamil-nadu" : "Tamil Nadu",
                               "telangana" : "Telangana",
                               "tripura" : "Tripura",
                               "uttar-pradesh" : "Uttar Pradesh",
                               "uttarakhand" : "Uttarakhand",
                               "west-bengal" : "West Bengal"})
    return top_trans

# Aggregated_User
def get_agg_user_details():
    path_state_agg_user = "C:\\Users\\harsh\\GUVI-Python\\Capstone_2\\Git_files\\data\\aggregated\\user\\country\\india"
    Agg_user_year_list = os.listdir(path_state_agg_user)
    path_state_agg_user = "C:\\Users\\harsh\\GUVI-Python\\Capstone_2\\Git_files\\data\\aggregated\\user\\country\\india\\state\\"
    Agg_user_state_list = os.listdir(path_state_agg_user)

    clm4 = {'State':[], 'Year':[],'Quater':[],'User_Brand':[], 'User_count':[], 'User_percentage':[]}

    for i in Agg_user_state_list:
        p_i = path_state_agg_user + i + "\\"
        Agg_yr = os.listdir(p_i)
        for j in Agg_yr:
            p_j = p_i + j + "/"
            Agg_yr_list = os.listdir(p_j)
            for k in Agg_yr_list:
                p_k = p_j+k
                Data = open(p_k,'r')
                D = json.load(Data)
                if D['data']['usersByDevice'] is not None:
                      for z in D['data']['usersByDevice']:
                          Brand = z["brand"]
                          Count = z["count"]
                          Percentage = z["percentage"]
                          clm4['User_Brand'].append(Brand)
                          clm4['User_count'].append(Count)
                          clm4['User_percentage'].append(Percentage)
                          clm4['State'].append(i)
                          clm4['Year'].append(j)
                          clm4['Quater'].append(int(k.strip('.json')))
    agg_user = pd.DataFrame(clm4)

    agg_user = agg_user.replace({"andaman-&-nicobar-islands" : "Andaman & Nicobar",
                               "andhra-pradesh" : "Andhra Pradesh",
                               "arunachal-pradesh" : "Arunachal Pradesh",
                               "assam" : "Assam",
                               "bihar" : "Bihar",
                               "chandigarh" : "Chandigarh",
                               "chhattisgarh" : "Chhattisgarh",
                               "dadra-&-nagar-haveli-&-daman-&-diu" : "Dadra and Nagar Haveli and Daman and Diu",
                               "delhi" : "Delhi",
                               "goa" : "Goa",
                               "gujarat" : "Gujarat",
                               "haryana" : "Haryana",
                               "himachal-pradesh" : "Himachal Pradesh",
                               "jammu-&-kashmir" : "Jammu & Kashmir",
                               "jharkhand" : "Jharkhand",
                               "karnataka" : "Karnataka",
                               "kerala" : "Kerala",
                               "ladakh" : "Ladakh",
                               "madhya-pradesh" : "Madhya Pradesh",
                               "maharashtra" : "Maharashtra",
                               "manipur" : "Manipur",
                               "meghalaya" : "Meghalaya",
                               "mizoram" : "Mizoram",
                               "nagaland" : "Nagaland",
                               "odisha" : "Odisha",
                               "puducherry" : "Puducherry",
                               "punjab" : "Punjab",
                               "rajasthan" : "Rajasthan",
                               "sikkim" : "Sikkim",
                               "tamil-nadu" : "Tamil Nadu",
                               "telangana" : "Telangana",
                               "tripura" : "Tripura",
                               "uttar-pradesh" : "Uttar Pradesh",
                               "uttarakhand" : "Uttarakhand",
                               "west-bengal" : "West Bengal"})
    return agg_user

# Map_User
def get_map_user_details():
    path_state_map_user = "C:\\Users\\harsh\\GUVI-Python\\Capstone_2\\Git_files\\data\\map\\user\\hover\\country\\india"
    Map_user_year_list = os.listdir(path_state_map_user)
    path_state_map_user = "C:\\Users\\harsh\\GUVI-Python\\Capstone_2\\Git_files\\data\\map\\user\\hover\\country\\india\\state\\"
    Map_user_state_list = os.listdir(path_state_map_user)

    clm5 = {'State':[], 'Year':[],'Quater':[],'District':[], 'Registered_Users':[], 'App_Opens':[]}

    for i in Map_user_state_list:
        p_i = path_state_map_user + i + "\\"
        Agg_yr = os.listdir(p_i)
        for j in Agg_yr:
            p_j = p_i + j + "/"
            Agg_yr_list = os.listdir(p_j)
            for k in Agg_yr_list:
                p_k = p_j+k
                Data = open(p_k,'r')
                D = json.load(Data)
                T = D['data']['hoverData']
                for district, info in T.items():
                  District = district
                  Registered_Users = info["registeredUsers"]
                  App_Opens = info["appOpens"] 
                  clm5['District'].append(District)
                  clm5['Registered_Users'].append(Registered_Users)
                  clm5['App_Opens'].append(App_Opens)
                  clm5['State'].append(i)
                  clm5['Year'].append(j)
                  clm5['Quater'].append(int(k.strip('.json')))
    map_user = pd.DataFrame(clm5)

    map_user = map_user.replace({"andaman-&-nicobar-islands" : "Andaman & Nicobar",
                               "andhra-pradesh" : "Andhra Pradesh",
                               "arunachal-pradesh" : "Arunachal Pradesh",
                               "assam" : "Assam",
                               "bihar" : "Bihar",
                               "chandigarh" : "Chandigarh",
                               "chhattisgarh" : "Chhattisgarh",
                               "dadra-&-nagar-haveli-&-daman-&-diu" : "Dadra and Nagar Haveli and Daman and Diu",
                               "delhi" : "Delhi",
                               "goa" : "Goa",
                               "gujarat" : "Gujarat",
                               "haryana" : "Haryana",
                               "himachal-pradesh" : "Himachal Pradesh",
                               "jammu-&-kashmir" : "Jammu & Kashmir",
                               "jharkhand" : "Jharkhand",
                               "karnataka" : "Karnataka",
                               "kerala" : "Kerala",
                               "ladakh" : "Ladakh",
                               "madhya-pradesh" : "Madhya Pradesh",
                               "maharashtra" : "Maharashtra",
                               "manipur" : "Manipur",
                               "meghalaya" : "Meghalaya",
                               "mizoram" : "Mizoram",
                               "nagaland" : "Nagaland",
                               "odisha" : "Odisha",
                               "puducherry" : "Puducherry",
                               "punjab" : "Punjab",
                               "rajasthan" : "Rajasthan",
                               "sikkim" : "Sikkim",
                               "tamil-nadu" : "Tamil Nadu",
                               "telangana" : "Telangana",
                               "tripura" : "Tripura",
                               "uttar-pradesh" : "Uttar Pradesh",
                               "uttarakhand" : "Uttarakhand",
                               "west-bengal" : "West Bengal"})
    return map_user

# Top_User
def get_top_user_details():
    path_state_top_user = "C:\\Users\\harsh\\GUVI-Python\\Capstone_2\\Git_files\\data\\top\\user\\country\\india"
    Top_user_year_list = os.listdir(path_state_top_user)
    path_state_top_user = "C:\\Users\\harsh\\GUVI-Python\\Capstone_2\\Git_files\\data\\top\\user\\country\\india\\state\\"
    Top_user_state_list = os.listdir(path_state_top_user)

    clm6 = {'State':[], 'Year':[],'Quater':[],'District_name':[], 'Registered_users':[]}

    for i in Top_user_state_list:
        p_i = path_state_top_user + i + "\\"
        Agg_yr = os.listdir(p_i)
        for j in Agg_yr:
            p_j = p_i + j + "/"
            Agg_yr_list = os.listdir(p_j)
            for k in Agg_yr_list:
                p_k = p_j+k
                Data = open(p_k,'r')
                D = json.load(Data)
                for z in D['data']['districts']:
                  Name = z['name']
                  RegisteredUsers =z['registeredUsers']
                  clm6['District_name'].append(Name)
                  clm6['Registered_users'].append(RegisteredUsers)
                  clm6['State'].append(i)
                  clm6['Year'].append(j)
                  clm6['Quater'].append(int(k.strip('.json')))
    top_user = pd.DataFrame(clm6)

    top_user = top_user.replace({"andaman-&-nicobar-islands" : "Andaman & Nicobar",
                               "andhra-pradesh" : "Andhra Pradesh",
                               "arunachal-pradesh" : "Arunachal Pradesh",
                               "assam" : "Assam",
                               "bihar" : "Bihar",
                               "chandigarh" : "Chandigarh",
                               "chhattisgarh" : "Chhattisgarh",
                               "dadra-&-nagar-haveli-&-daman-&-diu" : "Dadra and Nagar Haveli and Daman and Diu",
                               "delhi" : "Delhi",
                               "goa" : "Goa",
                               "gujarat" : "Gujarat",
                               "haryana" : "Haryana",
                               "himachal-pradesh" : "Himachal Pradesh",
                               "jammu-&-kashmir" : "Jammu & Kashmir",
                               "jharkhand" : "Jharkhand",
                               "karnataka" : "Karnataka",
                               "kerala" : "Kerala",
                               "ladakh" : "Ladakh",
                               "madhya-pradesh" : "Madhya Pradesh",
                               "maharashtra" : "Maharashtra",
                               "manipur" : "Manipur",
                               "meghalaya" : "Meghalaya",
                               "mizoram" : "Mizoram",
                               "nagaland" : "Nagaland",
                               "odisha" : "Odisha",
                               "puducherry" : "Puducherry",
                               "punjab" : "Punjab",
                               "rajasthan" : "Rajasthan",
                               "sikkim" : "Sikkim",
                               "tamil-nadu" : "Tamil Nadu",
                               "telangana" : "Telangana",
                               "tripura" : "Tripura",
                               "uttar-pradesh" : "Uttar Pradesh",
                               "uttarakhand" : "Uttarakhand",
                               "west-bengal" : "West Bengal"})
    return top_user

# Aggregated_Insurance
def get_agg_insurance_details():
    path_year_agg_insurance = "C:\\Users\\harsh\\GUVI-Python\\Capstone_2\\Git_files\\data\\aggregated\\insurance\\country\\india"
    Agg_insurance_year_list = os.listdir(path_year_agg_insurance)
    path_state_agg_insurance = "C:\\Users\\harsh\\GUVI-Python\\Capstone_2\\Git_files\\data\\aggregated\\insurance\\country\\india\\state\\"
    Agg_insurance_state_list = os.listdir(path_state_agg_insurance)

    clm7 = {'State':[], 'Year':[],'Quater':[],'Transacion_type':[], 'Transacion_count':[], 'Transacion_amount':[]}

    for i in Agg_insurance_state_list:
        p_i = path_state_agg_insurance + i + "\\"
        Agg_yr = os.listdir(p_i)
        for j in Agg_yr:
            p_j = p_i + j + "/"
            Agg_yr_list = os.listdir(p_j)
            for k in Agg_yr_list:
                p_k = p_j+k
                Data = open(p_k,'r')
                D = json.load(Data)
                for z in D['data']['transactionData']:
                  Name = z['name']
                  count = z['paymentInstruments'][0]['count']
                  amount = z['paymentInstruments'][0]['amount']
                  clm7['Transacion_type'].append(Name)
                  clm7['Transacion_count'].append(count)
                  clm7['Transacion_amount'].append(amount)
                  clm7['State'].append(i)
                  clm7['Year'].append(j)
                  clm7['Quater'].append(int(k.strip('.json')))
    agg_insurance = pd.DataFrame(clm7)

    agg_insurance = agg_insurance.replace({"andaman-&-nicobar-islands" : "Andaman & Nicobar",
                               "andhra-pradesh" : "Andhra Pradesh",
                               "arunachal-pradesh" : "Arunachal Pradesh",
                               "assam" : "Assam",
                               "bihar" : "Bihar",
                               "chandigarh" : "Chandigarh",
                               "chhattisgarh" : "Chhattisgarh",
                               "dadra-&-nagar-haveli-&-daman-&-diu" : "Dadra and Nagar Haveli and Daman and Diu",
                               "delhi" : "Delhi",
                               "goa" : "Goa",
                               "gujarat" : "Gujarat",
                               "haryana" : "Haryana",
                               "himachal-pradesh" : "Himachal Pradesh",
                               "jammu-&-kashmir" : "Jammu & Kashmir",
                               "jharkhand" : "Jharkhand",
                               "karnataka" : "Karnataka",
                               "kerala" : "Kerala",
                               "ladakh" : "Ladakh",
                               "madhya-pradesh" : "Madhya Pradesh",
                               "maharashtra" : "Maharashtra",
                               "manipur" : "Manipur",
                               "meghalaya" : "Meghalaya",
                               "mizoram" : "Mizoram",
                               "nagaland" : "Nagaland",
                               "odisha" : "Odisha",
                               "puducherry" : "Puducherry",
                               "punjab" : "Punjab",
                               "rajasthan" : "Rajasthan",
                               "sikkim" : "Sikkim",
                               "tamil-nadu" : "Tamil Nadu",
                               "telangana" : "Telangana",
                               "tripura" : "Tripura",
                               "uttar-pradesh" : "Uttar Pradesh",
                               "uttarakhand" : "Uttarakhand",
                               "west-bengal" : "West Bengal"})
    return agg_insurance

# Map_Insurance
def get_map_insurance_details():
    path_year_map_insurance = "C:\\Users\\harsh\\GUVI-Python\\Capstone_2\\Git_files\\data\\map\\insurance\\hover\\country\\india"
    Map_insurance_year_list = os.listdir(path_year_map_insurance)
    path_state_map_insurance = "C:\\Users\\harsh\\GUVI-Python\\Capstone_2\\Git_files\\data\\map\\insurance\\hover\\country\\india\\state\\"
    Map_insurance_state_list = os.listdir(path_state_map_insurance)

    clm8 = {'State':[], 'Year':[],'Quater':[],'District':[],'Transaction_count':[], 'Transaction_amount':[]}

    for i in Map_insurance_state_list:
        p_i = path_state_map_insurance + i + "\\"
        Agg_yr = os.listdir(p_i)
        for j in Agg_yr:
            p_j = p_i + j + "/"
            Agg_yr_list = os.listdir(p_j)
            for k in Agg_yr_list:
                p_k = p_j+k
                Data = open(p_k,'r')
                D = json.load(Data)    
                for T in D["data"]["hoverDataList"]:
                    name = T["name"]
                    count = T["metric"][0]["count"]
                    amount = T["metric"][0]["amount"]
                    clm8['State'].append(i)
                    clm8['Year'].append(j)
                    clm8['Quater'].append(int(k.strip('.json')))
                    clm8['District'].append(name)
                    clm8['Transaction_count'].append(count)
                    clm8['Transaction_amount'].append(amount)

    map_insurance = pd.DataFrame(clm8)
    map_insurance = map_insurance.replace({"andaman-&-nicobar-islands" : "Andaman & Nicobar",
                               "andhra-pradesh" : "Andhra Pradesh",
                               "arunachal-pradesh" : "Arunachal Pradesh",
                               "assam" : "Assam",
                               "bihar" : "Bihar",
                               "chandigarh" : "Chandigarh",
                               "chhattisgarh" : "Chhattisgarh",
                               "dadra-&-nagar-haveli-&-daman-&-diu" : "Dadra and Nagar Haveli and Daman and Diu",
                               "delhi" : "Delhi",
                               "goa" : "Goa",
                               "gujarat" : "Gujarat",
                               "haryana" : "Haryana",
                               "himachal-pradesh" : "Himachal Pradesh",
                               "jammu-&-kashmir" : "Jammu & Kashmir",
                               "jharkhand" : "Jharkhand",
                               "karnataka" : "Karnataka",
                               "kerala" : "Kerala",
                               "ladakh" : "Ladakh",
                               "madhya-pradesh" : "Madhya Pradesh",
                               "maharashtra" : "Maharashtra",
                               "manipur" : "Manipur",
                               "meghalaya" : "Meghalaya",
                               "mizoram" : "Mizoram",
                               "nagaland" : "Nagaland",
                               "odisha" : "Odisha",
                               "puducherry" : "Puducherry",
                               "punjab" : "Punjab",
                               "rajasthan" : "Rajasthan",
                               "sikkim" : "Sikkim",
                               "tamil-nadu" : "Tamil Nadu",
                               "telangana" : "Telangana",
                               "tripura" : "Tripura",
                               "uttar-pradesh" : "Uttar Pradesh",
                               "uttarakhand" : "Uttarakhand",
                               "west-bengal" : "West Bengal"})
    return map_insurance

# Top_Insurance
def get_top_insurance_details():
    path_year_top_insurance = "C:\\Users\\harsh\\GUVI-Python\\Capstone_2\\Git_files\\data\\top\\insurance\\country\\india"
    Top_insurance_year_list = os.listdir(path_year_top_insurance)
    path_state_top_insurance = "C:\\Users\\harsh\\GUVI-Python\\Capstone_2\\Git_files\\data\\top\\insurance\\country\\india\\state\\"
    Top_insurance_state_list = os.listdir(path_state_top_insurance)  

    clm9 = {'State':[], 'Year':[],'Quater':[], 'Pincodes':[], 'Transaction_count':[], 'Transaction_amount':[]}

    for i in Top_insurance_state_list:
        p_i = path_state_top_insurance + i + "\\"
        Agg_yr = os.listdir(p_i)
        for j in Agg_yr:
            p_j = p_i + j + "/"
            Agg_yr_list = os.listdir(p_j)
            for k in Agg_yr_list:
                p_k = p_j+k
                Data = open(p_k,'r')
                D = json.load(Data)    
                for T in D["data"]["pincodes"]:
                    name = T["entityName"]
                    count = T["metric"]["count"]
                    amount = T["metric"]["amount"]
                    clm9['State'].append(i)
                    clm9['Year'].append(j)
                    clm9['Quater'].append(int(k.strip('.json')))
                    clm9['Pincodes'].append(name)
                    clm9['Transaction_count'].append(count)
                    clm9['Transaction_amount'].append(amount)

    top_insurance = pd.DataFrame(clm9)

    top_insurance = top_insurance.replace({"andaman-&-nicobar-islands" : "Andaman & Nicobar",
                               "andhra-pradesh" : "Andhra Pradesh",
                               "arunachal-pradesh" : "Arunachal Pradesh",
                               "assam" : "Assam",
                               "bihar" : "Bihar",
                               "chandigarh" : "Chandigarh",
                               "chhattisgarh" : "Chhattisgarh",
                               "dadra-&-nagar-haveli-&-daman-&-diu" : "Dadra and Nagar Haveli and Daman and Diu",
                               "delhi" : "Delhi",
                               "goa" : "Goa",
                               "gujarat" : "Gujarat",
                               "haryana" : "Haryana",
                               "himachal-pradesh" : "Himachal Pradesh",
                               "jammu-&-kashmir" : "Jammu & Kashmir",
                               "jharkhand" : "Jharkhand",
                               "karnataka" : "Karnataka",
                               "kerala" : "Kerala",
                               "ladakh" : "Ladakh",
                               "madhya-pradesh" : "Madhya Pradesh",
                               "maharashtra" : "Maharashtra",
                               "manipur" : "Manipur",
                               "meghalaya" : "Meghalaya",
                               "mizoram" : "Mizoram",
                               "nagaland" : "Nagaland",
                               "odisha" : "Odisha",
                               "puducherry" : "Puducherry",
                               "punjab" : "Punjab",
                               "rajasthan" : "Rajasthan",
                               "sikkim" : "Sikkim",
                               "tamil-nadu" : "Tamil Nadu",
                               "telangana" : "Telangana",
                               "tripura" : "Tripura",
                               "uttar-pradesh" : "Uttar Pradesh",
                               "uttarakhand" : "Uttarakhand",
                               "west-bengal" : "West Bengal"})
    return top_insurance

### Data Cleaning    

Agg_Trans = get_agg_trans_details()
Agg_Trans.isnull().sum()
Agg_Trans["Year"] = Agg_Trans["Year"].astype(int)

Map_Trans = get_map_trans_details()
Map_Trans.isnull().sum()
Map_Trans["Year"] = Map_Trans["Year"].astype(int)

Top_Trans = get_top_trans_details()
Top_Trans.isnull().sum()
Top_Trans["Year"] = Top_Trans["Year"].astype(int)

Agg_User = get_agg_user_details()
Agg_User.isnull().sum()
Agg_User["Year"] = Agg_User["Year"].astype(int)

Map_User = get_map_user_details()
Map_User.isnull().sum()
Map_User["Year"] = Map_User["Year"].astype(int)

Top_User = get_top_user_details()
Top_User.isnull().sum()
Top_User["Year"] = Top_User["Year"].astype(int)

Agg_Insurance = get_agg_insurance_details()
Agg_Insurance.isnull().sum()
Agg_Insurance["Year"] = Agg_Insurance["Year"].astype(int)

Map_Insurance = get_map_insurance_details()
Map_Insurance.isnull().sum()
Map_Insurance["Year"] = Map_Insurance["Year"].astype(int)

Top_Insurance = get_top_insurance_details()
Top_Insurance.isnull().sum()
Top_Insurance["Year"] = Top_Insurance["Year"].astype(int)


### Insertion of data into MySQL


mycursor = mydb.cursor(buffered = True)

# Aggregated_Transaction_Table
mycursor.execute("SELECT DISTINCT State from Aggregate_Transaction_Details")
if mycursor.fetchone() is None:
    mycursor.execute("""CREATE TABLE Aggregate_Transaction_Details 
                    (State VARCHAR(255), Year INT, Quarter INT, Transaction_Type VARCHAR(255),Transaction_Count INT,Transaction_amount FLOAT)""")    

mydb.commit()

mycursor.execute("SELECT State from Aggregate_Transaction_Details LIMIT 5")
if mycursor.fetchone() is None:
    for index,row in Agg_Trans.iterrows():
        query = """INSERT INTO Aggregate_Transaction_Details (State, Year, Quarter, Transaction_Type, Transaction_Count, Transaction_amount) 
                    values (%s,%s,%s,%s,%s,%s)"""
        values = (row["State"],row["Year"],row["Quater"],row["Transacion_type"], row["Transacion_count"], row["Transacion_amount"])
        mycursor.execute(query,values)

mydb.commit()

# Map_Transaction_Table
mycursor.execute("SELECT DISTINCT State from Map_Transaction_Details")
if mycursor.fetchone() is None:
    mycursor.execute("""CREATE TABLE Map_Transaction_Details 
                    (State VARCHAR(255), Year INT, Quarter INT, Transaction_Type VARCHAR(255),Transaction_Count INT,Transaction_amount FLOAT)""")
    
mydb.commit()

mycursor.execute("SELECT State from Map_Transaction_Details LIMIT 5")
if mycursor.fetchone() is None:
    for index,row in Map_Trans.iterrows():
        query = """INSERT INTO Map_Transaction_Details (State, Year, Quarter, Transaction_Type, Transaction_Count, Transaction_amount) 
                    values (%s,%s,%s,%s,%s,%s)"""
        values = (row["State"],row["Year"],row["Quater"],row["Transacion_type"], row["Transacion_count"], row["Transacion_amount"])
        mycursor.execute(query,values)

mydb.commit()

# Top_Transaction_Table
mycursor.execute("SELECT DISTINCT State from Top_Transaction_Details")
if mycursor.fetchone() is None:
    mycursor.execute("""CREATE TABLE Top_Transaction_Details 
                    (State VARCHAR(255), Year INT, Quarter INT, Pincodes VARCHAR(255),Transaction_Count INT,Transaction_amount FLOAT)""")
    
mydb.commit()

mycursor.execute("SELECT State from Top_Transaction_Details LIMIT 5")
if mycursor.fetchone() is None:
    for index,row in Top_Trans.iterrows():
        query = """INSERT INTO Top_Transaction_Details (State, Year, Quarter, Pincodes, Transaction_Count, Transaction_amount) 
                    values (%s,%s,%s,%s,%s,%s)"""
        values = (row["State"],row["Year"],row["Quater"],row["Pincodes"], row["Transaction_count"], row["Transaction_amount"])
        mycursor.execute(query,values)

mydb.commit()

# Aggregated_User_Table
mycursor.execute("SELECT DISTINCT State from Aggregate_User_Details")
if mycursor.fetchone() is None:
    mycursor.execute("""CREATE TABLE Aggregate_User_Details 
                    (State VARCHAR(255), Year INT, Quarter INT, User_Brand VARCHAR(255),User_count INT,User_percentage FLOAT)""")
    
mydb.commit()

mycursor.execute("SELECT State from Aggregate_User_Details LIMIT 5")
if mycursor.fetchone() is None:
    for index,row in Agg_User.iterrows():
        query = """INSERT INTO Aggregate_User_Details (State, Year, Quarter, User_Brand, User_count, User_percentage) 
                    values (%s,%s,%s,%s,%s,%s)"""
        values = (row["State"],row["Year"],row["Quater"],row["User_Brand"], row["User_count"], row["User_percentage"])
        mycursor.execute(query,values)

mydb.commit()

# Map_User_Table
mycursor.execute("SELECT DISTINCT State from Map_User_Details")
if mycursor.fetchone() is None:
    mycursor.execute("""CREATE TABLE Map_User_Details 
                    (State VARCHAR(255), Year INT, Quarter INT, District VARCHAR(255),Registered_Users INT, App_Opens INT)""")
    
mydb.commit()

mycursor.execute("SELECT State from Map_User_Details LIMIT 5")
if mycursor.fetchone() is None:
    for index,row in Map_User.iterrows():
        query = """INSERT INTO Map_User_Details (State, Year, Quarter, District, Registered_Users,App_Opens ) 
                    values (%s,%s,%s,%s,%s,%s)"""
        values = (row["State"],row["Year"],row["Quater"],row["District"], row["Registered_Users"], row["App_Opens"])
        mycursor.execute(query,values)

mydb.commit()

# Top_User_Table
mycursor.execute("SELECT DISTINCT State from Top_User_Details")
if mycursor.fetchone() is None:
    mycursor.execute("""CREATE TABLE Top_User_Details 
                    (State VARCHAR(255), Year INT, Quarter INT, District_name VARCHAR(255),Registered_users INT)""")
    
mydb.commit()
mycursor.execute("SELECT State from Top_User_Details LIMIT 5")
if mycursor.fetchone() is None:
    for index,row in Top_User.iterrows():
        query = """INSERT INTO Top_User_Details (State, Year, Quarter, District_name, Registered_users) 
                    values (%s,%s,%s,%s,%s)"""
        values = (row["State"],row["Year"],row["Quater"],row["District_name"], row["Registered_users"])
        mycursor.execute(query,values)

mydb.commit()

# Aggregated_Insurance_Table
mycursor.execute("SELECT DISTINCT State from Aggregate_Insurance_Details")
if mycursor.fetchone() is None:
    mycursor.execute("""CREATE TABLE Aggregate_Insurance_Details 
                    (State VARCHAR(255), Year INT, Quarter INT, Transaction_Type VARCHAR(255),Transaction_Count INT,Transaction_amount FLOAT)""")
    
mydb.commit()
mycursor.execute("SELECT State from Aggregate_Insurance_Details LIMIT 5")
if mycursor.fetchone() is None:
    for index,row in Agg_Insurance.iterrows():
        query = """INSERT INTO Aggregate_Insurance_Details (State, Year, Quarter, Transaction_Type, Transaction_Count, Transaction_amount) 
                    values (%s,%s,%s,%s,%s,%s)"""
        values = (row["State"],row["Year"],row["Quater"],row["Transacion_type"], row["Transaction_count"], row["Transaction_amount"])
        mycursor.execute(query,values)

mydb.commit()

# Map_Insurance_Table
mycursor.execute("SELECT DISTINCT State from Map_Insurance_Details")
if mycursor.fetchone() is None:
    mycursor.execute("""CREATE TABLE Map_Insurance_Details 
                    (State VARCHAR(255), Year INT, Quarter INT, District VARCHAR(255),Transaction_Count INT,Transaction_amount FLOAT)""")
    
mydb.commit()
mycursor.execute("SELECT State from Map_Insurance_Details LIMIT 5")
if mycursor.fetchone() is None:
    for index,row in Map_Insurance.iterrows():
        query = """INSERT INTO Map_Insurance_Details (State, Year, Quarter, District, Transaction_Count, Transaction_amount) 
                    values (%s,%s,%s,%s,%s,%s)"""
        values = (row["State"],row["Year"],row["Quater"],row["District"], row["Transaction_count"], row["Transaction_amount"])
        mycursor.execute(query,values)

mydb.commit()

# Top_Insurance_Table
mycursor.execute("SELECT State from Top_Insurance_Details LIMIT 5")
if mycursor.fetchone() is None:
    mycursor.execute("""CREATE TABLE Top_Insurance_Details 
                    (State VARCHAR(255), Year INT, Quarter INT, Pincodes VARCHAR(255),Transaction_Count INT,Transaction_amount FLOAT)""")
    
mydb.commit()
mycursor.execute("SELECT State from Top_Insurance_Details LIMIT 5")
if mycursor.fetchone() is None:
    for index,row in Top_Insurance.iterrows():
        query = """INSERT INTO Top_Insurance_Details (State, Year, Quarter, Pincodes, Transaction_Count, Transaction_amount) 
                    values (%s,%s,%s,%s,%s,%s)"""
        values = (row["State"],row["Year"],row["Quater"],row["Pincodes"], row["Transaction_count"], row["Transaction_amount"])
        mycursor.execute(query,values)

mydb.commit()


### Data fetching from MYSQL and Data Frames Creation

mycursor.execute("SELECT * from Aggregate_Transaction_Details")
mydb.commit()
table_1 = mycursor.fetchall()
Agg_Trans_df = pd.DataFrame(table_1, columns = ("State", "Year", "Quarter", "Transaction_Type", "Transaction_Count", "Transaction_amount"))

mycursor.execute("SELECT * from Map_Transaction_Details")
mydb.commit()
table_2 = mycursor.fetchall()
Map_Trans_df = pd.DataFrame(table_2, columns = ("State", "Year", "Quarter", "District", "Transaction_Count", "Transaction_amount"))

mycursor.execute("SELECT * from Top_Transaction_Details")
mydb.commit()
table_3 = mycursor.fetchall()
Top_Trans_df = pd.DataFrame(table_3, columns = ("State", "Year", "Quarter", "Pincodes", "Transaction_Count", "Transaction_amount"))

mycursor.execute("SELECT * from Aggregate_User_Details")
mydb.commit()
table_4 = mycursor.fetchall()
Agg_User_df = pd.DataFrame(table_4, columns = ("State", "Year", "Quarter", "User_Brand", "User_count", "User_percentage"))

mycursor.execute("SELECT * from Map_User_Details")
mydb.commit()
table_5 = mycursor.fetchall()
Map_User_df = pd.DataFrame(table_5, columns = ("State", "Year", "Quarter", "District", "Registered_Users", "App_Opens"))

mycursor.execute("SELECT * from Top_User_Details")
mydb.commit()
table_6 = mycursor.fetchall()
Top_User_df = pd.DataFrame(table_6, columns = ("State", "Year", "Quarter", "Pincodes", "Registered_Users"))

mycursor.execute("SELECT * from Aggregate_Insurance_Details")
mydb.commit()
table_7 = mycursor.fetchall()
Agg_Insurance_df = pd.DataFrame(table_7, columns = ("State", "Year", "Quarter", "Transaction_Type", "Transaction_Count", "Transaction_amount"))

mycursor.execute("SELECT * from Map_Insurance_Details")
mydb.commit()
table_8 = mycursor.fetchall()
Map_Insurance_df = pd.DataFrame(table_8, columns = ("State", "Year", "Quarter", "District", "Transaction_Count", "Transaction_amount"))

mycursor.execute("SELECT * from Top_Insurance_Details")
mydb.commit()
table_9 = mycursor.fetchall()
Top_Insurance_df = pd.DataFrame(table_9, columns = ("State", "Year", "Quarter", "Pincodes", "Transaction_Count", "Transaction_amount"))


### Required function for different visualizations and Geo visualization

def get_cnt_amt_year_wise_details(df,year):


    agg_ins_amt_cnt = df[df["Year"] == year]
    agg_ins_amt_cnt.reset_index(drop = True, inplace = True)

    agg_ins_amt_cnt_grp = agg_ins_amt_cnt.groupby("State")[["Transaction_Count","Transaction_amount"]].sum()
    agg_ins_amt_cnt_grp.reset_index(inplace = True)

    col1, col2 = st.columns(2)

    with col1:
        fig_agg_ins_cnt = px.bar(agg_ins_amt_cnt_grp, x = "State", y = "Transaction_Count",title = f"TRANSACTION COUNT {year}", 
                            color_discrete_sequence = px.colors.sequential.algae_r, height = 650, width = 600)
        fig_agg_ins_cnt = fig_agg_ins_cnt.update_layout( xaxis = dict(tickangle = -90))
        st.plotly_chart(fig_agg_ins_cnt)
    
    with col2:
        fig_agg_ins_amt = px.bar(agg_ins_amt_cnt_grp, x = "State", y = "Transaction_amount",title = f"TRANSACTION  AMOUNT {year}", 
                            color_discrete_sequence = px.colors.sequential.Agsunset, height = 650, width = 600)
        fig_agg_ins_amt = fig_agg_ins_amt.update_layout( xaxis = dict(tickangle = -90))
        st.plotly_chart(fig_agg_ins_amt) 


    col1, col2 = st.columns(2)

    with col1:
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        res = requests.get(url)
        res_data = json.loads(res.content)
        states_list = []
        for i in res_data["features"]:
            states_list.append(i["properties"]["ST_NM"])

        states_list.sort()

        fig_india_map_1 = px.choropleth(agg_ins_amt_cnt_grp, geojson = res_data, locations = "State", featureidkey = "properties.ST_NM", 
                                color = "Transaction_Count", color_continuous_scale = "Temps", 
                                range_color = (agg_ins_amt_cnt_grp["Transaction_Count"].min(), agg_ins_amt_cnt_grp["Transaction_Count"].max()),
                                hover_name = "State", title = f"TRANSACTION COUNT {year} ", fitbounds = "locations",
                                height = 600, width = 600 
                                )

        fig_india_map_1.update_geos(visible = False)
        st.plotly_chart(fig_india_map_1)

    with col2:
        fig_india_map_2 = px.choropleth(agg_ins_amt_cnt_grp, geojson = res_data, locations = "State", featureidkey = "properties.ST_NM", 
                                    color = "Transaction_amount", color_continuous_scale = "Temps", 
                                    range_color = (agg_ins_amt_cnt_grp["Transaction_amount"].min(), agg_ins_amt_cnt_grp["Transaction_amount"].max()),
                                    hover_name = "State", title = f"TRANSACTION AMOUNT {year} ", fitbounds = "locations",
                                    height = 600, width = 600 
                                    )

        fig_india_map_2.update_geos(visible = False)
        st.plotly_chart(fig_india_map_2)
    
    return agg_ins_amt_cnt


def get_cnt_amt_qtr_wise_details(df,quarter):
    agg_ins_amt_cnt = df[df["Quarter"] == quarter]
    agg_ins_amt_cnt.reset_index(drop = True, inplace = True)

    agg_ins_amt_cnt_grp = agg_ins_amt_cnt.groupby("State")[["Transaction_Count","Transaction_amount"]].sum()
    agg_ins_amt_cnt_grp.reset_index(inplace = True)

    col1, col2 = st.columns(2)
    
    with col1:
        fig_agg_ins_cnt = px.bar(agg_ins_amt_cnt_grp, x = "State", y = "Transaction_Count",title = f"TRANSACTION COUNT {"Q"}{quarter} {df["Year"].min()}", 
                            color_discrete_sequence = px.colors.sequential.algae_r, height = 650, width = 600)
        fig_agg_ins_cnt = fig_agg_ins_cnt.update_layout( xaxis = dict(tickangle = -90))
        st.plotly_chart(fig_agg_ins_cnt)
    
    with col2:
        fig_agg_ins_amt = px.bar(agg_ins_amt_cnt_grp, x = "State", y = "Transaction_amount",title = f"TRANSACTION  AMOUNT {"Q"}{quarter} {df["Year"].min()}", 
                            color_discrete_sequence = px.colors.sequential.Agsunset, height = 650, width = 600 )
        fig_agg_ins_amt = fig_agg_ins_amt.update_layout( xaxis = dict(tickangle = -90))
        st.plotly_chart(fig_agg_ins_amt)
    
    col1, col2 = st.columns(2)

    with col1:
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        res = requests.get(url)
        res_data = json.loads(res.content)
        states_list = []
        for i in res_data["features"]:
            states_list.append(i["properties"]["ST_NM"])

        states_list.sort()

        fig_india_map_1 = px.choropleth(agg_ins_amt_cnt_grp, geojson = res_data, locations = "State", featureidkey = "properties.ST_NM", 
                                color = "Transaction_Count", color_continuous_scale = "Temps", 
                                range_color = (agg_ins_amt_cnt_grp["Transaction_Count"].min(), agg_ins_amt_cnt_grp["Transaction_Count"].max()),
                                hover_name = "State", title = f"TRANSACTION COUNT {"Q"}{quarter} {df["Year"].min()} ", fitbounds = "locations",
                                height = 600, width = 600 
                                )

        fig_india_map_1.update_geos(visible = False)
        st.plotly_chart(fig_india_map_1)

    with col2:
        fig_india_map_2 = px.choropleth(agg_ins_amt_cnt_grp, geojson = res_data, locations = "State", featureidkey = "properties.ST_NM", 
                                    color = "Transaction_amount", color_continuous_scale = "Temps", 
                                    range_color = (agg_ins_amt_cnt_grp["Transaction_amount"].min(), agg_ins_amt_cnt_grp["Transaction_amount"].max()),
                                    hover_name = "State", title = f"TRANSACTION AMOUNT {"Q"}{quarter} {df["Year"].min()} ", fitbounds = "locations",
                                    height = 600, width = 600 
                                    )

        fig_india_map_2.update_geos(visible = False)
        st.plotly_chart(fig_india_map_2)

    return agg_ins_amt_cnt


def get_agg_trans_cnt_amt_trans_type_wise_details(df, state):
    agg_trans_amt_cnt = df[df["State"] == state]
    agg_trans_amt_cnt.reset_index(drop = True, inplace = True)
    
    agg_trans_amt_cnt_grp = agg_trans_amt_cnt.groupby("Transaction_Type")[["Transaction_Count","Transaction_amount"]].sum()
    agg_trans_amt_cnt_grp.reset_index(inplace = True)

    col1, col2 = st.columns(2)

    with col1:
           
        fig_pie_chart_1 = px.pie(data_frame = agg_trans_amt_cnt_grp, names = "Transaction_Type", values = "Transaction_Count",
                                width = 600, title = f"TRANSACTION COUNT {"OF"} {state.upper()}", hole = 0.4)
        st.plotly_chart(fig_pie_chart_1)
    

    with col2:
            
        fig_pie_chart_2 = px.pie(data_frame = agg_trans_amt_cnt_grp, names = "Transaction_Type", values = "Transaction_amount",
                                width = 600, title = f"TRANSACTION AMOUNT {"OF"} {state.upper()}", hole = 0.4)
        st.plotly_chart(fig_pie_chart_2)
    
def get_agg_user_cnt_year_wise_details(df, year):
    agg_user_amt_cnt = df[df["Year"] == year]
    agg_user_amt_cnt.reset_index(drop = True, inplace = True)
    
    agg_user_amt_cnt_grp = pd.DataFrame(agg_user_amt_cnt.groupby("User_Brand")["User_count"].sum())
    agg_user_amt_cnt_grp.reset_index(inplace = True)
    
    fig_agg_user_cnt = px.bar(agg_user_amt_cnt_grp, x = "User_Brand", y = "User_count", title = f"BRANDS TRANSACTION COUNT {year}", 
                             width = 1100, color_discrete_sequence = px.colors.sequential.haline)
    
    st.plotly_chart(fig_agg_user_cnt)

    return agg_user_amt_cnt

def get_agg_user_cnt_qtr_wise_details(df, quarter):
    agg_user_amt_cnt_qtr = df[df["Quarter"] == quarter]
    agg_user_amt_cnt_qtr.reset_index(drop = True, inplace = True)
    
    agg_user_amt_cnt_qtr_grp = pd.DataFrame(agg_user_amt_cnt_qtr.groupby("User_Brand")["User_count"].sum())
    agg_user_amt_cnt_qtr_grp.reset_index(inplace = True)
    
    fig_agg_user_cnt_qtr = px.bar(agg_user_amt_cnt_qtr_grp, x = "User_Brand", y = "User_count", title = f"BRANDS TRANSACTION COUNT {"Q"}{quarter} {df["Year"].min()}",
                                   width = 1100, color_discrete_sequence = px.colors.sequential.Magenta_r)
    st.plotly_chart(fig_agg_user_cnt_qtr)

    return agg_user_amt_cnt_qtr

def get_agg_user_cnt_state_wise_details(df, state):
    agg_user_cnt_year_qtr_state_df = df[df["State"] == state]
    agg_user_cnt_year_qtr_state_df.reset_index(drop = True, inplace = True)
    
    fig_line_1 = px.line(agg_user_cnt_year_qtr_state_df, x = "User_Brand", y = "User_count", hover_data = "User_percentage",
                         title = f"BRANDS , TRANSACTION COUNT AND PERCENTAGE OF {state.upper()}", width = 1000, markers = True)
    st.plotly_chart(fig_line_1)
    
    
def get_map_ins_cnt_amt_district_wise_details(df, state):
    map_ins_amt_cnt = df[df["State"] == state]
    map_ins_amt_cnt.reset_index(drop = True, inplace = True)
    
    map_ins_amt_cnt_grp = map_ins_amt_cnt.groupby("District")[["Transaction_Count","Transaction_amount"]].sum()
    map_ins_amt_cnt_grp.reset_index(inplace = True)
    
    col1, col2 = st.columns(2)

    with col1:
        fig_bar_chart_1 = px.bar(map_ins_amt_cnt_grp, x = "Transaction_Count", y = "District", orientation = "h", 
                                    title = f"DISTRICT WISE TRANSACTION COUNT OF {state.upper()}", 
                                    color_discrete_sequence = px.colors.sequential.Mint_r, height = 600)
        st.plotly_chart(fig_bar_chart_1)
    
    with col2:
        fig_bar_chart_2 = px.bar(map_ins_amt_cnt_grp, x = "Transaction_amount", y = "District", orientation = "h", 
                                title = f"DISTRICT WISE TRANSACTION AMOUNT OF {state.upper()}", 
                                color_discrete_sequence = px.colors.sequential.Mint_r, height = 600)
        st.plotly_chart(fig_bar_chart_2)
        

def get_map_user_reg_app_details(df, year):
    map_user_amt_cnt = df[df["Year"] == year]
    map_user_amt_cnt.reset_index(drop = True, inplace = True)
    
    map_user_amt_cnt_grp = pd.DataFrame(map_user_amt_cnt.groupby("State")[["Registered_Users","App_Opens"]].sum())
    map_user_amt_cnt_grp.reset_index(inplace = True)
    
    fig_line_1 = px.line(map_user_amt_cnt_grp, x = "State", y = ["Registered_Users","App_Opens"],
                         title = f"STATE WISE REGISTERED USERS & APP OPENS {year}", width = 1000, height = 800, markers = True)
    st.plotly_chart(fig_line_1)

    return map_user_amt_cnt


def get_map_user_reg_app_qtr_details(df, quarter):
    map_user_amt_cnt_qtr = df[df["Quarter"] == quarter]
    map_user_amt_cnt_qtr.reset_index(drop = True, inplace = True)
    
    map_user_amt_cnt_qtr_grp = pd.DataFrame(map_user_amt_cnt_qtr.groupby("State")[["Registered_Users","App_Opens"]].sum())
    map_user_amt_cnt_qtr_grp.reset_index(inplace = True)
    
    fig_line_1 = px.line(map_user_amt_cnt_qtr_grp, x = "State", y = ["Registered_Users","App_Opens"],
                         title = f"STATE WISE REGISTERED USERS & APP OPENS {"Q"}{quarter} {df["Year"].min()}", width = 1000, height = 800,
                         markers = True, color_discrete_sequence = px.colors.sequential.Rainbow_r)
    st.plotly_chart(fig_line_1)

    return map_user_amt_cnt_qtr


def get_map_user_reg_app_qtr_district_wise_details(df, state):
    map_user_amt_cnt_qtr_state = map_user_cnt_amt_qtr_df[map_user_cnt_amt_qtr_df["State"] == state]
    map_user_amt_cnt_qtr_state.reset_index(drop = True, inplace = True)
    
    fig_bar_chart_1 = px.bar(map_user_amt_cnt_qtr_state, x = "Registered_Users", y = "District", orientation = "h", 
                             title = f"DISTRICT WISE REGISTERED USERS OF {state.upper()}", 
                             color_discrete_sequence = px.colors.sequential.Mint_r, height = 600)
    st.plotly_chart(fig_bar_chart_1)
    
    fig_bar_chart_1 = px.bar(map_user_amt_cnt_qtr_state, x = "App_Opens", y = "District", orientation = "h", 
                             title = f"DISTRICT WISE APP OPENS OF {state.upper()}", 
                             color_discrete_sequence = px.colors.sequential.Mint_r, height = 600)
    st.plotly_chart(fig_bar_chart_1)

    
def get_top_ins_cnt_amt_district_wise_details(df, state):
    top_ins_amt_cnt_state = df[df["State"] == state]
    top_ins_amt_cnt_state.reset_index(drop = True, inplace = True)
    
    
    top_ins_amt_cnt_state_grp = pd.DataFrame(top_ins_amt_cnt_state.groupby("Pincodes")[["Transaction_Count","Transaction_amount"]].sum())
    top_ins_amt_cnt_state_grp.reset_index(inplace = True)

    col1, col2 = st.columns(2)

    with col1:   
        fig_bar_chart_1 = px.bar(top_ins_amt_cnt_state, x = "Quarter", y = "Transaction_Count", 
                                title = f"PINCODE WISE TRANSACTION COUNT OF {state.upper()}", 
                                color_discrete_sequence = px.colors.sequential.Rainbow, height = 650, width = 600,)
        st.plotly_chart(fig_bar_chart_1)
    
    with col2:
        fig_bar_chart_2 = px.bar(top_ins_amt_cnt_state, x = "Quarter", y = "Transaction_amount",
                                title = f"PINCODE WISE TRANSACTION AMOUNT OF {state.upper()}", 
                                color_discrete_sequence = px.colors.sequential.GnBu_r, height = 650, width = 600,)
        st.plotly_chart(fig_bar_chart_2)

def get_top_user_reg_details(df,year):
    top_user_amt_cnt = df[df["Year"] == year]
    top_user_amt_cnt.reset_index(drop = True, inplace = True)
    
    top_user_amt_cnt_grp = pd.DataFrame(top_user_amt_cnt.groupby(["State","Quarter"])["Registered_Users"].sum())
    top_user_amt_cnt_grp.reset_index(inplace = True)
    
    fig_top_plot_1 = px.bar(top_user_amt_cnt_grp, x = "State", y = "Registered_Users", color = "Quarter",title = f"REGISTERED USERS {year}",
                        width = 1000, height = 800,color_discrete_sequence = px.colors.sequential.Burgyl)
    fig_top_plot_1 = fig_top_plot_1.update_layout( xaxis = dict(tickangle = -90))
    
    st.plotly_chart(fig_top_plot_1)

    return top_user_amt_cnt    

    
def get_top_user_reg_state_wise_details(df, state):
    top_user_amt_cnt_state = df[df["State"] == state]
    top_user_amt_cnt_state.reset_index(drop = True, inplace = True)
    
    fig_top_user_plot_1 = px.bar(top_user_amt_cnt_state, x = "Quarter", y = "Registered_Users", 
                                 title = f"REGISTERED USERS, QUARTER AND PINCODE DETAILS of {state.upper()}",
                                 width = 1000, height = 800, color = "Registered_Users", hover_data = "Pincodes", 
                                 color_continuous_scale = px.colors.sequential.Magenta)
    st.plotly_chart(fig_top_user_plot_1)
        


def get_top_chart_trans_cnt_details(table_name):
    query1 = f"""select State, SUM(Transaction_Count) As TRANSACTION_COUNT
                from {table_name} 
                GROUP BY State
                ORDER BY Transaction_Count DESC
                LIMIT 10;"""
    mycursor.execute(query1)
    var1 = mycursor.fetchall()
    mydb.commit()
    
    df_top_1 = pd.DataFrame(var1, columns = ("State","Transaction_Count"))
    
    col1, col2 = st.columns(2)

    with col1:
        fig_top_chart_1 = px.bar(df_top_1, x = "State", y = "Transaction_Count", 
                                title = "TOP 10 TRANSACTION COUNT", color_continuous_scale = px.colors.sequential.Magenta,
                                height = 650, width = 600)
        fig_top_chart_1 = fig_top_chart_1.update_layout( xaxis = dict(tickangle = -90))
        st.plotly_chart(fig_top_chart_1)
    
    
    query2 = f"""select State, SUM(Transaction_Count) As TRANSACTION_COUNT
                from {table_name}  
                GROUP BY State
                ORDER BY Transaction_Count
                LIMIT 10;"""
    mycursor.execute(query2)
    var2 = mycursor.fetchall()
    mydb.commit()
    
    df_top_2 = pd.DataFrame(var2, columns = ("State","Transaction_Count"))
    
    with col2:

        fig_top_chart_2 = px.bar(df_top_2, x = "State", y = "Transaction_Count", 
                                title = "BOTTOM 10 TRANSACTION COUNT", color_continuous_scale = px.colors.sequential.Aggrnyl_r,
                                height = 650, width = 600)
        fig_top_chart_2 = fig_top_chart_2.update_layout( xaxis = dict(tickangle = -90))
        st.plotly_chart(fig_top_chart_2)
    
    
    
    query3 = f"""select State, AVG(Transaction_Count) As TRANSACTION_COUNT
                from {table_name}  
                GROUP BY State
                ORDER BY Transaction_Count
                """
    mycursor.execute(query3)
    var3 = mycursor.fetchall()
    mydb.commit()
    
    df_top_3 = pd.DataFrame(var3, columns = ("State","Transaction_Count"))
    
    fig_top_chart_3 = px.bar(df_top_3, x = "Transaction_Count", y = "State", 
                             title = "AVERAGE TRANSACTION COUNT", color_continuous_scale = px.colors.sequential.Aggrnyl, orientation = "h",
                             height = 800, width = 1000)
    fig_top_chart_3 = fig_top_chart_3.update_layout( xaxis = dict(tickangle = -90))
    st.plotly_chart(fig_top_chart_3)

def get_top_chart_trans_amt_details(table_name):
    query1 = f"""select State, SUM(Transaction_amount) As TRANSACTION_AMOUNT
                from {table_name} 
                GROUP BY State
                ORDER BY Transaction_amount DESC
                LIMIT 10;"""
    mycursor.execute(query1)
    var1 = mycursor.fetchall()
    mydb.commit()
    
    df_top_1 = pd.DataFrame(var1, columns = ("State","Transaction_amount"))
    
    col1, col2 = st.columns(2)

    with col1:
        fig_top_chart_1 = px.bar(df_top_1, x = "State", y = "Transaction_amount", 
                                title = "TOP 10 TRANSACTION AMOUNT", color_continuous_scale = px.colors.sequential.Magenta,
                                height = 650, width = 600)
        fig_top_chart_1 = fig_top_chart_1.update_layout( xaxis = dict(tickangle = -90))
        st.plotly_chart(fig_top_chart_1)
        

    query2 = f"""select State, SUM(Transaction_amount) As TRANSACTION_AMOUNT
                from {table_name}  
                GROUP BY State
                ORDER BY Transaction_amount
                LIMIT 10;"""
    mycursor.execute(query2)
    var2 = mycursor.fetchall()
    mydb.commit()
    
    df_top_2 = pd.DataFrame(var2, columns = ("State","Transaction_amount"))
    
    with col2 :
        fig_top_chart_2 = px.bar(df_top_2, x = "State", y = "Transaction_amount", 
                                title = "BOTTOM 10 TRANSACTION AMOUNT", color_continuous_scale = px.colors.sequential.Aggrnyl_r,
                                height = 650, width = 600)
        fig_top_chart_2 = fig_top_chart_2.update_layout( xaxis = dict(tickangle = -90))
        st.plotly_chart(fig_top_chart_2)
    
    
    
    query3 = f"""select State, AVG(Transaction_amount) As TRANSACTION_AMOUNT
                from {table_name}  
                GROUP BY State
                ORDER BY Transaction_amount
                """
    mycursor.execute(query3)
    var3 = mycursor.fetchall()
    mydb.commit()
    
    df_top_3 = pd.DataFrame(var3, columns = ("State","Transaction_amount"))
    
    fig_top_chart_3 = px.bar(df_top_3, x = "Transaction_amount", y = "State", 
                             title = "AVERAGE TRANSACTION AMOUNT", color_continuous_scale = px.colors.sequential.Aggrnyl, orientation = "h",
                             height = 800, width = 1000)
    fig_top_chart_3 = fig_top_chart_3.update_layout( xaxis = dict(tickangle = -90))
    st.plotly_chart(fig_top_chart_3)


def get_top_chart_agg_user_trans_cnt_details(table_name):
    query1 = f"""select State, SUM(User_count) As User_count
                from {table_name} 
                GROUP BY State
                ORDER BY User_count DESC
                LIMIT 10;"""
    mycursor.execute(query1)
    var1 = mycursor.fetchall()
    mydb.commit()
    
    df_top_1 = pd.DataFrame(var1, columns = ("State","User_count"))
    
    fig_top_chart_1 = px.bar(df_top_1, x = "State", y = "User_count", 
                             title = "USER COUNT", color_continuous_scale = px.colors.sequential.Magenta,
                             height = 650, width = 600)
    fig_top_chart_1 = fig_top_chart_1.update_layout( xaxis = dict(tickangle = -90))
    st.plotly_chart(fig_top_chart_1)
    
    
    query2 = f"""select State, SUM(User_count) As User_count
                from {table_name}  
                GROUP BY State
                ORDER BY User_count
                LIMIT 10;"""
    mycursor.execute(query2)
    var2 = mycursor.fetchall()
    mydb.commit()
    
    df_top_2 = pd.DataFrame(var2, columns = ("State","User_count"))
    
    fig_top_chart_2 = px.bar(df_top_2, x = "State", y = "User_count", 
                             title = "USER COUNT", color_continuous_scale = px.colors.sequential.Aggrnyl_r,
                             height = 650, width = 600)
    fig_top_chart_2 = fig_top_chart_2.update_layout( xaxis = dict(tickangle = -90))
    st.plotly_chart(fig_top_chart_2)
    
    
    
    query3 = f"""select State, AVG(User_count) As User_count
                from {table_name}  
                GROUP BY State
                ORDER BY User_count
                """
    mycursor.execute(query3)
    var3 = mycursor.fetchall()
    mydb.commit()
    
    df_top_3 = pd.DataFrame(var3, columns = ("State","User_count"))
    
    fig_top_chart_3 = px.bar(df_top_3, x = "User_count", y = "State", 
                             title = "USER COUNT", color_continuous_scale = px.colors.sequential.Aggrnyl, orientation = "h",
                             height = 800, width = 1000)
    fig_top_chart_3 = fig_top_chart_3.update_layout( xaxis = dict(tickangle = -90))
    st.plotly_chart(fig_top_chart_3)

def get_top_chart_map_user_reg_users_details(table_name, state_name):
    query1 = f"""select District, SUM(Registered_Users) As Registered_Users
                from {table_name}
                where State = '{state_name}'
                GROUP BY District
                ORDER BY Registered_Users DESC
                LIMIT 10;"""
    mycursor.execute(query1)
    var1 = mycursor.fetchall()
    mydb.commit()
    
    df_top_1 = pd.DataFrame(var1, columns = ("District","Registered_Users"))

    col1, col2 = st.columns(2)

    with col1:
        fig_top_chart_1 = px.bar(df_top_1, x = "District", y = "Registered_Users", 
                                title = "TOP 10 REGISTERED USERS", color_continuous_scale = px.colors.sequential.Magenta,
                                height = 650, width = 600)
        fig_top_chart_1 = fig_top_chart_1.update_layout( xaxis = dict(tickangle = -90))
        st.plotly_chart(fig_top_chart_1)
    
    
    query2 = f"""select District, SUM(Registered_Users) As Registered_Users
                from {table_name}
                where State = '{state_name}'
                GROUP BY District
                ORDER BY Registered_Users
                LIMIT 10;"""
    mycursor.execute(query2)
    var2 = mycursor.fetchall()
    mydb.commit()
    
    df_top_2 = pd.DataFrame(var2, columns = ("District","Registered_Users"))
    
    with col2:
        fig_top_chart_2 = px.bar(df_top_2, x = "District", y = "Registered_Users", 
                                title = "BOTTOM 10 REGISTERED USERS", color_continuous_scale = px.colors.sequential.Aggrnyl_r,
                                height = 650, width = 600)
        fig_top_chart_2 = fig_top_chart_2.update_layout( xaxis = dict(tickangle = -90))
        st.plotly_chart(fig_top_chart_2)
    
    
    
    query3 = f"""select District, AVG(Registered_Users) As Registered_Users
                from {table_name}
                where State = '{state_name}'
                GROUP BY District
                """
    mycursor.execute(query3)
    var3 = mycursor.fetchall()
    mydb.commit()
    
    df_top_3 = pd.DataFrame(var3, columns = ("District","Registered_Users"))
    
    fig_top_chart_3 = px.bar(df_top_3, x = "Registered_Users", y = "District", 
                             title = "AVERAGE REGISTERED USERS", color_continuous_scale = px.colors.sequential.Aggrnyl, orientation = "h",
                             height = 800, width = 1000)
    fig_top_chart_3 = fig_top_chart_3.update_layout( xaxis = dict(tickangle = -90))
    st.plotly_chart(fig_top_chart_3)


def get_top_chart_map_user_app_opens_details(table_name, state_name):
    query1 = f"""select District, SUM(App_Opens) As App_Opens
                from {table_name}
                where State = '{state_name}'
                GROUP BY District
                ORDER BY App_Opens DESC
                LIMIT 10;"""
    mycursor.execute(query1)
    var1 = mycursor.fetchall()
    mydb.commit()
    
    df_top_1 = pd.DataFrame(var1, columns = ("District","App_Opens"))
    
    col1, col2 = st.columns(2)

    with col1:
        fig_top_chart_1 = px.bar(df_top_1, x = "District", y = "App_Opens", 
                                title = "TOP 10 APP OPENS", color_continuous_scale = px.colors.sequential.Magenta,
                                height = 650, width = 600)
        fig_top_chart_1 = fig_top_chart_1.update_layout( xaxis = dict(tickangle = -90))
        st.plotly_chart(fig_top_chart_1)
        
    
    query2 = f"""select District, SUM(App_Opens) As App_Opens
                from {table_name}
                where State = '{state_name}'
                GROUP BY District
                ORDER BY App_Opens
                LIMIT 10;"""
    mycursor.execute(query2)
    var2 = mycursor.fetchall()
    mydb.commit()
    
    df_top_2 = pd.DataFrame(var2, columns = ("District","App_Opens"))
    
    with col2:
        fig_top_chart_2 = px.bar(df_top_2, x = "District", y = "App_Opens", 
                                title = "BOTTOM 10 APP OPENS", color_continuous_scale = px.colors.sequential.Aggrnyl_r,
                                height = 650, width = 600)
        fig_top_chart_2 = fig_top_chart_2.update_layout( xaxis = dict(tickangle = -90))
        st.plotly_chart(fig_top_chart_2)
    
    
    
    query3 = f"""select District, AVG(App_Opens) As App_Opens
                from {table_name}
                where State = '{state_name}'
                GROUP BY District
                """
    mycursor.execute(query3)
    var3 = mycursor.fetchall()
    mydb.commit()
    
    df_top_3 = pd.DataFrame(var3, columns = ("District","App_Opens"))
    
    fig_top_chart_3 = px.bar(df_top_3, x = "App_Opens", y = "District", 
                             title = "AVERAGE APP OPENS", color_continuous_scale = px.colors.sequential.Aggrnyl, orientation = "h",
                             height = 800, width = 1000)
    fig_top_chart_3 = fig_top_chart_3.update_layout( xaxis = dict(tickangle = -90))
    st.plotly_chart(fig_top_chart_3)



def get_top_chart_top_user_reg_users_details(table_name):
    query1 = f"""select State, SUM(Registered_Users) As Registered_Users
                from {table_name}
                GROUP BY State
                ORDER BY Registered_Users DESC
                LIMIT 10;"""
    mycursor.execute(query1)
    var1 = mycursor.fetchall()
    mydb.commit()
    
    df_top_1 = pd.DataFrame(var1, columns = ("State","Registered_Users"))

    col1, col2 = st.columns(2)

    with col1:
        fig_top_chart_1 = px.bar(df_top_1, x = "State", y = "Registered_Users", 
                                title = "TOP 10 REGISTERED USERS", color_continuous_scale = px.colors.sequential.Magenta,
                                height = 650, width = 600)
        fig_top_chart_1 = fig_top_chart_1.update_layout( xaxis = dict(tickangle = -90))
        st.plotly_chart(fig_top_chart_1)
    
    
    query2 = f"""select State, SUM(Registered_Users) As Registered_Users
                from {table_name}
                GROUP BY State
                ORDER BY Registered_Users
                LIMIT 10;"""
    mycursor.execute(query2)
    var2 = mycursor.fetchall()
    mydb.commit()
    
    df_top_2 = pd.DataFrame(var2, columns = ("State","Registered_Users"))

    with col2:
        fig_top_chart_2 = px.bar(df_top_2, x = "State", y = "Registered_Users", 
                                title = "BOTTOM 10 REGISTERED USERS", color_continuous_scale = px.colors.sequential.Aggrnyl_r,
                                height = 650, width = 600)
        fig_top_chart_2 = fig_top_chart_2.update_layout( xaxis = dict(tickangle = -90))
        st.plotly_chart(fig_top_chart_2)
    
    
    
    query3 = f"""select State, AVG(Registered_Users) As Registered_Users
                from {table_name}
                GROUP BY State
                """
    mycursor.execute(query3)
    var3 = mycursor.fetchall()
    mydb.commit()
    
    df_top_3 = pd.DataFrame(var3, columns = ("State","Registered_Users"))
    
    fig_top_chart_3 = px.bar(df_top_3, x = "Registered_Users", y = "State", 
                             title = "AVERAGE REGISTERED USERS", color_continuous_scale = px.colors.sequential.Aggrnyl, orientation = "h",
                             height = 800, width = 1000)
    fig_top_chart_3 = fig_top_chart_3.update_layout( xaxis = dict(tickangle = -90))
    st.plotly_chart(fig_top_chart_3)



### Streamlit Part

st.set_page_config(layout = "wide")
st.title("PhonePe Pulse | THE BEAT OF PROGRESS")

with st.sidebar:
    select = option_menu("Main Menu",["Home", "Data Exploration", "Top Charts"])

if select == "Home":
    
        col1,col2= st.columns(2)

        with col1:
            st.header("PHONEPE")
            st.subheader("INDIA'S BEST TRANSACTION APP")
            st.markdown("PhonePe  is an Indian digital payments and financial technology company")
            st.write("****FEATURES****")
            st.write("****Credit & Debit card linking****")
            st.write("****Bank Balance check****")
            st.write("****Money Storage****")
            st.write("****PIN Authorization****")
            st.download_button("DOWNLOAD THE APP NOW", "https://www.phonepe.com/app-download/")
        
        with col2:
            st.image(Image.open(r"C:\Users\harsh\GUVI-Python\Capstone_2\Phonepe Images\th (2).jpeg"),width= 600)

        col3,col4= st.columns(2)
        
        with col3:
            st.write("    ")
            st.write("    ")
            st.image(Image.open(r"C:\Users\harsh\GUVI-Python\Capstone_2\Phonepe Images\th (1).jpeg"),width= 600)

        with col4:
            st.write("****Easy Transactions****")
            st.write("****One App For All Your Payments****")
            st.write("****Your Bank Account Is All You Need****")
            st.write("****Multiple Payment Modes****")
            st.write("****PhonePe Merchants****")
            st.write("****Multiple Ways To Pay****")
            st.write("****1.Direct Transfer & More****")
            st.write("****2.QR Code****")
            st.write("****Earn Great Rewards****")

        col5,col6= st.columns(2)

        with col5:
            st.markdown(" ")
            st.markdown(" ")
            st.markdown(" ")
            st.markdown(" ")
            st.markdown(" ")
            st.markdown(" ")
            st.markdown(" ")
            st.write("****No Wallet Top-Up Required****")
            st.write("****Pay Directly From Any Bank To Any Bank A/C****")
            st.write("****Instantly & Free****")

        with col6:
             st.write(" ")
             st.write(" ")
             st.image(Image.open(r"C:\Users\harsh\GUVI-Python\Capstone_2\Phonepe Images\th (3).jpeg"),width= 600)

        col7, col8 = st.columns(2)

        with col7:
            video_url = "https://www.youtube.com/watch?v=QG6iEwlnPoE"
            st.video(video_url)

elif select == "Data Exploration":
    tab_1,tab_2,tab_3 = st.tabs(["Aggregated Analysis", "Map Analysis", "Top Analysis"])

    with tab_1:
        option_1 = st.radio("Select the option",["Aggregated Insurance", "Aggregated Transaction", "Aggregated User"])
        
        if option_1 == "Aggregated Insurance":

            col1, col2 = st.columns(2)
            with col1:
                #year_selection = st.slider("Select the year", Agg_Insurance_df["Year"].unique().min(), Agg_Insurance_df["Year"].unique().max(), Agg_Insurance_df["Year"].unique().min())
                year_selection = st.selectbox("Select the year", Agg_Insurance_df["Year"].unique())
            agg_ins_cnt_amt_df = get_cnt_amt_year_wise_details(Agg_Insurance_df, year_selection)

            col1, col2 = st.columns(2)
            
            with col1:
                quarter_selection = st.selectbox("Select the quarter", agg_ins_cnt_amt_df["Quarter"].unique())
            get_cnt_amt_qtr_wise_details(agg_ins_cnt_amt_df,quarter_selection)


        elif option_1 == "Aggregated Transaction":

            col1, col2 = st.columns(2)

            with col1:
                    year_selection = st.selectbox("Select the year", Agg_Trans_df["Year"].unique())
            agg_trans_cnt_amt_df = get_cnt_amt_year_wise_details(Agg_Trans_df, year_selection)

            col1,col2 = st.columns(2)

            with col1:

                state_selection = st.selectbox("Select the State", agg_trans_cnt_amt_df["State"].unique())
            get_agg_trans_cnt_amt_trans_type_wise_details(agg_trans_cnt_amt_df, state_selection)

            col1, col2 = st.columns(2)
            
            with col1:
                quarter_selection = st.selectbox("Select the quarter here", agg_trans_cnt_amt_df["Quarter"].unique())
            agg_trans_cnt_amt_qtr_df = get_cnt_amt_qtr_wise_details(agg_trans_cnt_amt_df, quarter_selection)

            col1,col2 = st.columns(2)

            with col1:

                state_selection = st.selectbox("Select the State here", agg_trans_cnt_amt_qtr_df["State"].unique())
            get_agg_trans_cnt_amt_trans_type_wise_details(agg_trans_cnt_amt_qtr_df, state_selection)


        elif option_1 == "Aggregated User":

            col1, col2 = st.columns(2)

            with col1:
                    year_selection = st.selectbox("Select the year", Agg_User_df["Year"].unique())
            agg_user_cnt_df = get_agg_user_cnt_year_wise_details(Agg_User_df, year_selection)

            col1, col2 = st.columns(2)
            
            with col1:
                quarter_selection = st.selectbox("Select the quarter here", agg_user_cnt_df["Quarter"].unique())
            agg_user_cnt_qtr_df = get_agg_user_cnt_qtr_wise_details(agg_user_cnt_df, quarter_selection)

            col1,col2 = st.columns(2)

            with col1:

                state_selection = st.selectbox("Select the State ", agg_user_cnt_qtr_df["State"].unique())
            get_agg_user_cnt_state_wise_details(agg_user_cnt_qtr_df, state_selection)           

    with tab_2:
        option_2 = st.radio("Select the option",["Map Insurance", "Map Transaction", "Map User"])

        if option_2 == "Map Insurance":
            
            col1, col2 = st.columns(2)

            with col1:
                    year_selection = st.selectbox("Select the Year", Map_Insurance_df["Year"].unique())
            map_ins_cnt_amt_df = get_cnt_amt_year_wise_details(Map_Insurance_df,year_selection)

            col1,col2 = st.columns(2)

            with col1:

                state_selection = st.selectbox("Select the State", map_ins_cnt_amt_df["State"].unique())
            get_map_ins_cnt_amt_district_wise_details(map_ins_cnt_amt_df, state_selection)

            col1, col2 = st.columns(2)
            
            with col1:
                quarter_selection = st.selectbox("Select the Quarter here", map_ins_cnt_amt_df["Quarter"].unique())
            map_ins_cnt_amt_qtr_df = get_cnt_amt_qtr_wise_details(map_ins_cnt_amt_df,quarter_selection)

            col1,col2 = st.columns(2)

            with col1:

                state_selection = st.selectbox("Select the State here ", map_ins_cnt_amt_qtr_df["State"].unique())
            get_map_ins_cnt_amt_district_wise_details(map_ins_cnt_amt_qtr_df, state_selection)

        elif option_2 == "Map Transaction":
            col1, col2 = st.columns(2)

            with col1:
                    year_selection = st.selectbox("Select the Year", Map_Trans_df["Year"].unique())
            map_trans_cnt_amt_df = get_cnt_amt_year_wise_details(Map_Trans_df,year_selection)

            col1,col2 = st.columns(2)

            with col1:

                state_selection = st.selectbox("Select the State", map_trans_cnt_amt_df["State"].unique())
            get_map_ins_cnt_amt_district_wise_details(map_trans_cnt_amt_df, state_selection)

            col1, col2 = st.columns(2)
            
            with col1:
                quarter_selection = st.selectbox("Select the Quarter here", map_trans_cnt_amt_df["Quarter"].unique())
            map_trans_cnt_amt_qtr_df = get_cnt_amt_qtr_wise_details(map_trans_cnt_amt_df,quarter_selection)

            col1,col2 = st.columns(2)

            with col1:

                state_selection = st.selectbox("Select the State here ", map_trans_cnt_amt_qtr_df["State"].unique())
            get_map_ins_cnt_amt_district_wise_details(map_trans_cnt_amt_qtr_df, state_selection)

        elif option_2 == "Map User":
            col1, col2 = st.columns(2)

            with col1:
                    year_selection = st.selectbox("Select the Year", Map_User_df["Year"].unique())
            map_user_cnt_amt_df = get_map_user_reg_app_details(Map_User_df, year_selection)

            col1, col2 = st.columns(2)
            
            with col1:
                quarter_selection = st.selectbox("Select the Quarter here", map_user_cnt_amt_df["Quarter"].unique())
            map_user_cnt_amt_qtr_df = get_map_user_reg_app_qtr_details(map_user_cnt_amt_df, quarter_selection)

            col1,col2 = st.columns(2)

            with col1:

                state_selection = st.selectbox("Select the State here ", map_user_cnt_amt_qtr_df["State"].unique())
            get_map_user_reg_app_qtr_district_wise_details(map_user_cnt_amt_qtr_df, state_selection)


    with tab_3:
        option_3 = st.radio("Select the option",["Top Insurance", "Top Transaction", "Top User"])

        if option_3 == "Top Insurance":
            
            col1, col2 = st.columns(2)

            with col1:
                    year_selection = st.selectbox("Select the Year ", Top_Insurance_df["Year"].unique())
            top_ins_cnt_amt_df = get_cnt_amt_year_wise_details(Top_Insurance_df, year_selection)
        
            col1,col2 = st.columns(2)

            with col1:

                state_selection = st.selectbox("Select the State   ", top_ins_cnt_amt_df["State"].unique())
            get_top_ins_cnt_amt_district_wise_details(top_ins_cnt_amt_df, state_selection)
        
            col1, col2 = st.columns(2)
            
            with col1:
                quarter_selection = st.selectbox("Select the Quarter here  ", top_ins_cnt_amt_df["Quarter"].unique())
            top_ins_cnt_amt_qtr_df = get_cnt_amt_qtr_wise_details(top_ins_cnt_amt_df,quarter_selection)
        
        elif option_3 == "Top Transaction":
            col1, col2 = st.columns(2)

            with col1:
                    year_selection = st.selectbox("Select the Year  ", Top_Trans_df["Year"].unique())
            top_trans_cnt_amt_df = get_cnt_amt_year_wise_details(Top_Trans_df, year_selection)
        
            col1,col2 = st.columns(2)

            with col1:

                state_selection = st.selectbox("Select the State    ", top_trans_cnt_amt_df["State"].unique())
            get_top_ins_cnt_amt_district_wise_details(top_trans_cnt_amt_df, state_selection)
        
            col1, col2 = st.columns(2)
            
            with col1:
                quarter_selection = st.selectbox("Select the Quarter here   ", top_trans_cnt_amt_df["Quarter"].unique())
            top_trans_cnt_amt_qtr_df = get_cnt_amt_qtr_wise_details(top_trans_cnt_amt_df,quarter_selection) 
           
        elif option_3 == "Top User":
            
            col1, col2 = st.columns(2)

            with col1:
                    year_selection = st.selectbox("Select the Year   ", Top_User_df["Year"].unique())
            top_user_cnt_amt_df = get_top_user_reg_details(Top_User_df,year_selection )

            col1,col2 = st.columns(2)

            with col1:

                state_selection = st.selectbox("Select the State here   ", top_user_cnt_amt_df["State"].unique())
            get_top_user_reg_state_wise_details(top_user_cnt_amt_df, state_selection)


elif select == "Top Charts":
    question = st.selectbox("Select the Question", [ "1. Transaction Amount and Count of Aggregated Insurance",
                                                    "2. Transaction Amount and Count of Map Insurance",
                                                    "3. Transaction Amount and Count of Top Insurance",
                                                    "4. Transaction Amount and Count of Aggregated Transaction",
                                                    "5. Transaction Amount and Count of Map Transaction",
                                                    "6. Transaction Amount and Count of Top Transaction",
                                                    "7. Transaction Count of Aggregated Users",
                                                    "8. Registered Users of Map User",
                                                    "9. App Opens of Map User",
                                                    "10. Registered Users of Top Users"]
                                                    )
    
    if question == "1. Transaction Amount and Count of Aggregated Insurance":

        st.subheader("TRANSACTION COUNT")
        get_top_chart_trans_cnt_details("aggregate_insurance_details")

        st.subheader("TRANSACTION AMOUNT")
        get_top_chart_trans_amt_details("aggregate_insurance_details")
    
    elif question == "2. Transaction Amount and Count of Map Insurance":
        
        st.subheader("TRANSACTION COUNT")
        get_top_chart_trans_cnt_details("map_insurance_details")

        st.subheader("TRANSACTION AMOUNT")
        get_top_chart_trans_amt_details("map_insurance_details")

    elif question == "3. Transaction Amount and Count of Top Insurance":

        st.subheader("TRANSACTION COUNT")
        get_top_chart_trans_cnt_details("top_insurance_details")

        st.subheader("TRANSACTION AMOUNT")
        get_top_chart_trans_amt_details("top_insurance_details")

    elif question == "4. Transaction Amount and Count of Aggregated Transaction":

        st.subheader("TRANSACTION COUNT")
        get_top_chart_trans_cnt_details("aggregate_transaction_details")

        st.subheader("TRANSACTION AMOUNT")
        get_top_chart_trans_amt_details("aggregate_transaction_details")

    elif question == "5. Transaction Amount and Count of Map Transaction":

        st.subheader("TRANSACTION COUNT")
        get_top_chart_trans_cnt_details("map_transaction_details")

        st.subheader("TRANSACTION AMOUNT")
        get_top_chart_trans_amt_details("map_transaction_details")

    elif question == "6. Transaction Amount and Count of Top Transaction":

        st.subheader("TRANSACTION COUNT")
        get_top_chart_trans_cnt_details("top_transaction_details")

        st.subheader("TRANSACTION AMOUNT")
        get_top_chart_trans_amt_details("top_transaction_details")

    elif question == "7. Transaction Count of Aggregated Users":

        st.subheader("TRANSACTION COUNT")
        get_top_chart_agg_user_trans_cnt_details("aggregate_user_details")

    elif question == "8. Registered Users of Map User":

        state_name = st.selectbox("Select the State     ", Map_User_df["State"].unique()) 
        
        st.subheader("REGISTERED USERS")
        get_top_chart_map_user_reg_users_details("map_user_details", state_name)

    elif question == "9. App Opens of Map User":

        state_name = st.selectbox("Select the State      ", Map_User_df["State"].unique()) 
        
        st.subheader("APP OPENS")
        get_top_chart_map_user_app_opens_details("map_user_details", state_name)

    elif question == "10. Registered Users of Top Users":

        st.subheader("REGISTERED USERS")
        get_top_chart_top_user_reg_users_details("top_user_details")
    



