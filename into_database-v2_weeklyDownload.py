payload = {
    'UserID': 'MWPSA',
    'UserPWD': 'nsd1018',
    'SearchType': '1'
    }

add_num=500000


















import requests
from bs4 import BeautifulSoup
import csv
import time
import random
import re
import sqlite3
from sqlite3 import Error
urlTo_valSystem= r"C:\Users\info\Documents\PSAavm\Code and Data"   #./
website = 'http://www.mwpic.com.hk/CheckLogin.php'




def create_connection(database_file):
    conn = None
    try:
        conn = sqlite3.connect(database_file)
        return conn
    except:
        print('no database file found')


# In[3]:


def create_table(conn,table_to_be_added):
    try:
        c = conn.cursor()
        c.execute(table_to_be_added)
    except Error as e:
        print(e)


# In[4]:


def obtain_data(request_object):


    '''
    Find Memorial Number /Can sepearte into a function
    '''
        #def obtain_memo_incum(url)

    #request_html = session.get(url) #get the transaction page html
    #request_html.encoding = 'Big5-HKSCS'
    #print(request_html.text)

    r_text = request_object.text #Since the incum no locate within tag, we turn the html into string instead of souping it

    memo_no_index = r_text.find('Memorial No:') # Only 1 'Memorial No:''
    incum_no_index = r_text.find('Incumbrances',8000) # we need to locate the 2nd word of "incumbrances"

    #print(r_text)

    soup = BeautifulSoup(request_object.content, 'html.parser', from_encoding = 'Big5-HKSCS') #Soup it for easier data extraction
    transact_data_group = soup.select(".LB")
    #print(memo_no_index)
    #print(transact_data_group)
    #print(data_code)
    #print(url)
    #print(transact_data_group)
    #print(transact_data_group[1])

    memo_no = transact_data_group[1].get_text() #memo no is always the 2nd data
    print(memo_no,data_code)

    #type(memo_data_group[1])


    if soup.find('input', {'value':'Incumbrances'})!= None:  #There may be no incumbrance
        print('we are in')
        incum_no_tag = soup.find('input', {'value':'Incumbrances'})
        incum_tag_string = str(incum_no_tag)
        #print(type(incum_no_tag))

        incum_no = incum_tag_string[incum_tag_string.find('(')+2 : incum_tag_string.find(')')-1]# slice the tag for incum no
        #print(incum_no)

    '''
    Obtain transaction data /Can sepearte into a function
    '''

    search_page = session.get('http://www.mwpic.com.hk/Pparesult.php?MEM_NO=' + memo_no + '&ORDER=D&FID=1', allow_redirects=True)
    search_result = session.get('http://www.mwpic.com.hk/Pparesult.php?ORDER=D')
    excel_html = session.get('http://www.mwpic.com.hk/Ppaexport.php?RECNO=' +data_code +'-')
    excel_html.encoding = 'Big5-HKSCS'



    #print(excel_html.text)

    excel_soup = BeautifulSoup(excel_html.content,'html.parser', from_encoding = 'Big5-HKSCS')




    transcat_data = excel_soup.find_all('td')

    try: #Handle no incumbrance situation
        incum_html = session.get('http://www.mwpic.com.hk/Incumbrances.php?PRN='+ incum_no)
        incum_html.encoding = 'Big5-HKSCS'
        incum_soup = BeautifulSoup(incum_html.content,'html.parser', from_encoding = 'Big5-HKSCS')
        incum_data = incum_soup.select('.LB')


        incum_text = [lb.get_text().strip() for lb in incum_data]
        #print(incum_text)

        polish_incum=[]
        for item in incum_text:
            polish_incum.append(item.replace("\nA:-", "").replace("C:", "").replace("MESSRS.", "").replace('$ ', "").replace("-", "").replace("\nS:",''))
    except:
        polish_incum=[]
        print('no incum list to add')

    original_transact = [td.get_text() for td in transcat_data]
    polish_transact = original_transact[0:27]
    polish_transact.append(data_code)
    #print(polish_transact)
    #print(polish_incum)

    data_raw = polish_transact + polish_incum
    #no_list = list(enumerate(data_raw))
    #print(no_list)

    return data_raw


# In[5]:


def insert_data(conn, sql_code, data_be_added):
    try:
        c=conn.cursor()
        c.execute(sql_code, data_be_added) #turple
    except Error as e:
        print(e)



# In[6]:


def add_to_all_tables(conn, data_list):
    d= data_list
    try:
        memo_no = int(d[0])
    except:
        memo_no = int(d[0][2:])
    nat_instr = d[1]
    print(nat_instr)
    instr_date = d[2] # need to be formated with Datetime
    usage = d[3]
    print(usage)
    cp_incl = d[4]
    unit = d[5]
    floor = d[6]
    block = d[7]
    phase = d[8]
    bldg_name_en= d[9]
    bldg_name_ch= d[10]
    est_name_en = d[11]
    est_name_ch = d[12]
    st_en = d[13]
    st_ch = d[14]
    st_from = d[15]
    st_to = d[16]
    lot_no = d[17]
    d_code = d[18]
    op_date = d[19]
    consid = d[20]
    gfa = d[21]
    nfa = d[22]
    gpsf = d[23]
    npsf = d[24]
    remark1 = d[25].replace("\r\n", "") ##
    remark2 = d[26].replace("\r\n", "") ##
    mw_data_code = d[27]

    # add incumbrance fields
    try:
        full_address = d[31]

        i_memo = []
        i_inst_date= []
        i_deli_date = []
        i_content = []
        i_consid =[]
        i_solic = []


        for data in d[34::8]:
            i_memo.append(data)
        i_memo.reverse()
        for data in d[35::8]:
            i_inst_date.append(data)
        i_inst_date.reverse()
        for data in d[36::8]:
            i_deli_date.append(data)
        i_deli_date.reverse()
        for data in d[37::8]:
            i_content.append(data)
        i_content.reverse()
        for data in d[38::8]:
            i_consid.append(data)
        i_consid.reverse()
        for data in d[39::8]:
            i_solic.append(data)
        i_solic.reverse()
    except:
        print('no incumbrance found')
        web_login=session.post(website, data=payload) #re-login to prevent a log out after checking the an incumbrance link
        pass

    #Insert address table
    address_sql = '''INSERT INTO address VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ;'''
    address_data = (memo_no,unit,floor,block,phase,bldg_name_en,bldg_name_ch, est_name_en, est_name_ch, st_en, st_ch, st_from, st_to, d_code,lot_no)
    insert_data (conn, address_sql, address_data)

    #transact_table

    transact_sql = '''INSERT INTO transaction_data VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?);'''
    transact_data = (memo_no,nat_instr,instr_date, usage,cp_incl,op_date,consid,gfa,nfa,gpsf,npsf,remark1,remark2,mw_data_code)
    insert_data (conn, transact_sql, transact_data)



    #incum_table
    incum_sql = '''INSERT INTO incumbrance VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ;'''

    try:
        incum_no = min(len(i_memo),10)
        incum_list = [memo_no]
        #print(incum_no)
        for i in range(incum_no):
            incum_list.append(i_memo[i])
            incum_list.append(i_inst_date[i])
            incum_list.append(i_deli_date[i])
            incum_list.append(i_content[i])
            #print(incum_list)
            incum_list.append(i_consid[i])
            incum_list.append(i_solic[i])
        #print(incum_list)
        while True:
            if len(incum_list)>60:
                break
            incum_list.append("")
        incum_data = tuple(incum_list)
        insert_data (conn, incum_sql, incum_data)
    except:
        print("shall have no incumbrance")
        memo_no_for_incum = (memo_no,)
        insert_data (conn,"INSERT INTO incumbrance(memo_no) VALUES(?);" , memo_no_for_incum)





# In[12]:


def main():



    """
    Setup Section:
    """
    global database_file
    
    database_file = urlTo_valSystem +r"\MW_database_MAY2020_all_district.db"
    conn = create_connection(database_file)
    with conn:

        #Create various tables
        address_table = """CREATE TABLE IF NOT EXISTS address(s
                                        memo_no integer PRIMARY KEY,
                                        unit char(5),
                                        floor char(5),
                                        block char(10),
                                        phase char(10),
                                        bldg_name_en varchar(100),
                                        bldg_name_ch varchar(50),
                                        est_name_en varchar(100),
                                        est_name_ch varchar(50),
                                        st_name_en varchar(50),
                                        st_name_ch varchar(15),
                                        st_no_start char(10),
                                        st_no_end char(10),
                                        d_code char(5),
                                        lot_no varchar(100)


    );"""

        create_table(conn, address_table)


        #Create Transaction Detail table
        transact_table = """CREATE TABLE IF NOT EXISTS transaction_data (
                                        memo_no integer PRIMARY KEY,
                                        nat_instr char(10),
                                        instr_date char(15),
                                        usage char (10),
                                        cp_incl char(5),
                                        op_date char(20),
                                        consid varchar(10),
                                        gfa char(10),
                                        nfa char(10),
                                        gpsf char(10),
                                        npsf char(10),
                                        remark1 varchar(50),
                                        remark2 varchar(100),
                                        MW_data_code small int
    );"""

        #FOREIGN KEY (memo_no) REFERENCES address (memo_no)
        create_table(conn, transact_table)

        #Create Incum Table
        incum_table = """CREATE TABLE IF NOT EXISTS incumbrance(
                                        memo_no integer PRIMARY KEY,
                                        incum_memo_no1 integer,
                                        inst_date_no1 char(12),
                                        deli_date_no1 char(12),
                                        content_no1 varchar(100),
                                        consid_no1 varchar(10),
                                        solic_no1 varchar(50),
                                        incum_memo_no2 integer,
                                        inst_date_no2 char(12),
                                        deli_date_no2 char(12),
                                        content_no2 varchar(100),
                                        consid_no2 varchar(10),
                                        solic_no2 varchar(50),
                                        incum_memo_no3 integer,
                                        inst_date_no3 char(12),
                                        deli_date_no3 char(12),
                                        content_no3 varchar(100),
                                        consid_no3 varchar(10),
                                        solic_no3 varchar(50),
                                        incum_memo_no4 integer,
                                        inst_date_no4 char(12),
                                        deli_date_no4 char(12),
                                        content_no4 varchar(100),
                                        consid_no4 varchar(10),
                                        solic_no4 varchar(50),
                                        incum_memo_no5 integer,
                                        inst_date_no5 char(12),
                                        deli_date_no5 char(12),
                                        content_no5 varchar(100),
                                        consid_no5 varchar(10),
                                        solic_no5 varchar(50),
                                        incum_memo_no6 integer,
                                        inst_date_no6 char(12),
                                        deli_date_no6 char(12),
                                        content_no6 varchar(100),
                                        consid_no6 varchar(10),
                                        solic_no6 varchar(50),
                                        incum_memo_no7 integer,
                                        inst_date_no7 char(12),
                                        deli_date_no7 char(12),
                                        content_no7 varchar(100),
                                        consid_no7 varchar(10),
                                        solic_no7 varchar(50),
                                        incum_memo_no8 integer,
                                        inst_date_no8 char(12),
                                        deli_date_no8 char(12),
                                        content_no8 varchar(100),
                                        consid_no8 varchar(10),
                                        solic_no8 varchar(50),
                                        incum_memo_no9 integer,
                                        inst_date_no9 char(12),
                                        deli_date_no9 char(12),
                                        content_no9 varchar(100),
                                        consid_no9 varchar(10),
                                        solic_no9 varchar(50),
                                        incum_memo_no10 integer,
                                        inst_date_no10 char(12),
                                        deli_date_no10 char(12),
                                        content_no10 varchar(100),
                                        consid_no10 varchar(10),
                                        solic_no10 varchar(50)
    );"""
        create_table(conn, incum_table)


    


#MWCHFT
#mw0902

    """
    Looping Section:
    """
    #########################################Obtain Data#############################################

    #5601597 5599199 5599036 5598996-5598990 miss incum 5594196 discover 'Big5-HKSCS' 5588552 5584803 5579796 5574148 5571569
    #5571103 5562237(maybe used re from here??) 5555851 5548605 5541538 5533393 5524550 5510016 fixed encoding 5503662 5492620
    #5492281 5491866 5488120 5480663 5474458
    #restart with KMK only, begin from 5474458 5461689 5440040 5364801 5347692 5326200 5305532 5293648 5266176 5158706 5125849
    #5087326 restart after pwd changed 5114733 replace to MW_code2: 5112299 5102318 5085985 5083463 4981760 4944139 4914691
    #4896594 4865889 4644388 4585939 4529489 4472454 4428153 4328337 4310128 4263119 4214469 4166151 4109810 4053498 3968976
    #3909213 3900496 3852740 3799493 3784832 3759637 3529107 3465899 3400496 3329861 3261342 3134971 3086895 3001612
    #2823360 2517733 2449516 2380203 2312451 2244729 2122935 2071902 2057362 1982848 1921602 1830749 1779922 1689148
    #1622681 1569912 1504697 1304920 1212802 1146047 761104 516756 471734 314194 223065 5659050
    # Please redo 5535610 - 5474458, may missed
    global session


    #Curl='http://www.mwpic.com.hk/'

    #home_page = requests.get(Curl)
    #home_text= home_page.text
    #code_check = re.search("onclick\=\"showinfo\((\d+)\)", home_text)
    #last_code = int(code_check.group(1))


    conn = create_connection(database_file)
    cur = conn.cursor()
    cur.execute("""SELECT MW_data_code FROM transaction_data ORDER BY MW_data_code DESC LIMIT 1""")
    #for selection: WHERE address.memo_no = 19090501380014
    rows = cur.fetchall()
    last_code=int(rows[0][-1])

    start_range = last_code-0  #5658881 #=last_code-700, end_code=last_code+200 (700,200 to be confirmed)
    end_code=last_code+ add_num
    jump_range = 1000
    time_wait = random.randint(1,2)
    count=0


    #while start_range >0:

    global proxy
    session = requests.session()

    print('start at:',start_range)
    #get_Host_name_IP() #Function call

    web_login=session.post(website, data=payload)

    no_match_counter=0

    for i in range(start_range, end_code,1):  #largest code is 5608658 as at 8 SEP 2019
        global data_code
        if i==end_code:
            print("Done")



        elif no_match_counter==200: ####################Try again, need test


            #raise ValueError
            break
        #global url

        data_code = str(i) # data_code will be used again in other function
        url = 'http://www.mwpic.com.hk/Ppaview.php?ID='+ data_code.zfill(7)
        print('to download',i)

        try:
            request_html = session.get(url)
            time.sleep(time_wait) # wait a while to call again
        except:
            print('OH~~~~ConnectionError')
            time.sleep(1800)
            request_html = session.get(url)

        soup = BeautifulSoup(request_html.content, 'html.parser') #Soup it for easier data extraction

        transact_data_group = soup.select(".LB")

        txt = soup.get_text()

        #print(txt)
        login_fail_check = re.search(".*(ErrorFont)", txt)
        if login_fail_check:
            print('login failed',login_fail_check)
            time.sleep(900)
            web_login=session.post(website, data=payload)
            request_html = session.get(url)
            soup = BeautifulSoup(request_html.content, 'html.parser') #Soup it for easier data extraction
            transact_data_group = soup.select(".LB")
            txt = soup.get_text()
            login_fail_check = re.search(".*(ErrorFont)", txt)
            if login_fail_check:
                print('login failure persist,end download')
                break

        instr_check = re.search(".*([^-]ASP|PRO-ASP|MEM-ASP|PRE-ASP|SUB-ASP)", txt)
        #district_check = re.search(".*(KMK)", txt)
        #print(district_check.group(1))
        #usage_check = re.search(".*(RES|COM)",txt)

        if instr_check: #and usage_check
            print("YES! We have a match!")
            no_match_counter=0
            #time.sleep( time_wait )
        else:
            print("No match and next")
            no_match_counter+=1
            time.sleep( 0.5 )
            continue

        #if district_check:
        #    print('It is in KMK')
        #else:
        #    print('not in KMK')
        #    with open(r"C:\Users\Terry\Dropbox\temp_code.txt", "a+") as f:
        #        f.write(data_code+ " ")
        #    time.sleep( 0.5 )
        #    continue


        if  transact_data_group[1].get_text()=="":
            continue


        try:
            data_list = obtain_data(request_html) #obtain data list

        except:
            print("####fails to obtain data;shall have no entry at all#####")
            continue

        #print(data_list)



        #Insert Data

        try:
            add_to_all_tables(conn, data_list)
        except:
            print("#####add value fails######")
            try:  ##database design mistake, memo_no only accept integer but actually not
                memoNo=int(data_list[0])
            except:
                #with open(r"C:\Users\Terry\Dropbox\nonInt_memoNo.txt", 'a+') as f:
                #    f.write("%a: %s " % (data_code, data_list[0]) )
                continue
            break
            #i-=1



        conn.commit()
        count+=1
        print(count,'end_code',end_code)


        #start_range += jump_range
        print(start_range)
        time.sleep( time_wait )




if __name__ == '__main__':
    try:
        main()

    except:
        with open(urlTo_valSystem+r"\end_code.txt", "a+") as f:
            f.write("up to"+data_code)    #!/usr/bin/env python








    ############################################# Process Data after downloaded new data ######################################################################


    #from google.colab import files
    import pandas as pd
    import sqlite3, datetime,time,requests,re,os
    import numpy as np
    from itertools import combinations

    import seaborn as sns
    import matplotlib.pyplot as plt

    import sklearn
    from sklearn.linear_model import LinearRegression

    #get_ipython().system('pip install regressors')
    #from regressors import stats

    #path for local running
    #urlTo_valSystem= r"C:\Users\info\Documents\PSAavm\Code and Data"


    # In[2]:


    district_df = pd.read_csv(urlTo_valSystem+ '/val_sys/district_list.csv', index_col=0, squeeze=True)
    code_dict=district_df.set_index('d_code')['code_chinese'].to_dict()


    # In[3]:


    from sqlite3 import Error

    def create_connection(database_file):
        conn = None
        try:
            conn = sqlite3.connect(database_file)
            return conn
        except:
            print('no database file found')


    # # Generate raw df

    # In[4]:


    database_file = urlTo_valSystem+r"\MW_database_MAY2020_all_district.db"
    conn = create_connection(database_file)

    for district in district_df['d_code'][:]:
        #district='NTY'
        print(district)
        #district = district_df.iloc[0,0]
        district_str="'"+ district + "'"
        sql_code= """SELECT *
                        FROM transaction_data
                        INNER JOIN address ON transaction_data.memo_no=address.memo_no
                        WHERE transaction_data.usage = 'RES' and address.d_code={}

                        """.format(district_str)
        kmk_df = pd.read_sql_query(sql_code, conn)
        #kmk_df is a subset of the database
        #INNER JOIN incumbrance ON transaction_data.memo_no = incumbrance.memo_no

        kmk_df['nfa'] = pd.to_numeric(kmk_df['nfa'])
        kmk_df['npsf'] = pd.to_numeric(kmk_df['npsf'])
        kmk_df['gfa'] = pd.to_numeric(kmk_df['gfa'])
        kmk_df['gpsf'] = pd.to_numeric(kmk_df['gpsf'])
        kmk_df['consid'] = pd.to_numeric(kmk_df['consid'])


        kmk = kmk_df.copy()  #make a copy for contingency

        # Add bldg list
        kmk= kmk.iloc[:,list(range(0,29))]
        kmk['bldg_ph_blk'] = kmk['est_name_en']+"|"+kmk['bldg_name_en']+kmk['bldg_name_ch']+"|Phase "+kmk['phase']+"|Block "+kmk['block']
        kmk['st_bldg_ph_blk'] =kmk['st_name_ch'] + kmk['est_name_en']+kmk['bldg_name_en']+kmk['bldg_name_ch']+"|Phase"+kmk['phase']+"|Block"+kmk['block']
        kmk['subject_pty']= kmk['st_name_ch']+kmk['est_name_en']+kmk['bldg_name_en']+kmk['bldg_name_ch']+"|Phase"+kmk['phase']+"|Block"+kmk['block']+'|'+kmk['floor'].astype(str)+'|'+kmk['unit']

        kmk=kmk.replace(r'^\s*$', 'blank', regex=True) #replace ' ' with 'blank' for better character management


        kmk['est_name_ch']=kmk['est_name_ch'].replace('blank', '') # To avoid displaying address with 'blank' inside, since we use not 'blank' english column for primary selection, eng columns no need this
        kmk['bldg_name_ch']=kmk['bldg_name_ch'].replace('blank', '')
        #print(kmk.columns)

        # To cater devleopment only has Eng name
        kmk_replace1= kmk[(kmk['est_name_ch']=="") &(kmk['est_name_en']!='blank')]
        replace1_index=kmk_replace1.index
        for ind_1 in replace1_index.to_list():
            kmk.loc[ind_1,'est_name_ch']=kmk.loc[ind_1,'est_name_en']

        kmk_replace2= kmk[(kmk['bldg_name_ch']=="") &(kmk['bldg_name_en']!='blank')]
        replace2_index=kmk_replace2.index
        for ind_2 in replace2_index.to_list():
            kmk.loc[ind_2,'bldg_name_ch']=kmk.loc[ind_2,'bldg_name_en']


        kmk['unit']=kmk['unit'].replace('blank', 'whole floor')
        kmk= kmk[kmk['unit'].notnull()]
        kmk=kmk.replace(r'(?:,|\r\n|\r|\r\r)', '', regex=True) #replace /r/n,/r,/r/r with ''


        #remove no street name transaction & replace blank with ''
        kmk=kmk.drop(kmk[(kmk['st_name_en']=='blank')].index)
        kmk['st_no_start']=kmk['st_no_start'].replace('blank', ' ')
        #kmk['st_no_start']=kmk['st_no_start'].astype(int)

        # Save to google drive
        print('datalen',len(kmk))

        kmk.to_csv(urlTo_valSystem+ '/raw_data/full_{}_df.csv'.format(district)) 


    # In[ ]:


    bldg_no=0
    dist_counter=0
    for district in district_df['d_code'][dist_counter:]:
        #district="NTY"
        print(district, dist_counter)
        dist_counter+=1

        url_group = urlTo_valSystem+ '/raw_data/full_{}_df.csv'.format(district)
        kmk_group= pd.read_csv(url_group, keep_default_na=False, index_col =0)
        ##pd.read_csv(StringIO(data), sep=' ', keep_default_na=False, na_values=['_'])

        #Replace Abbrevation
        kmk_group['est_name_en']=kmk_group['est_name_en'].replace({' GDN': ' GARDEN'}, regex=True)
        kmk_group['bldg_name_en']=kmk_group['bldg_name_en'].replace({' GDN': ' GARDEN'}, regex=True)
        kmk_group['est_name_en']=kmk_group['est_name_en'].replace({' MANSION': ' MANS'}, regex=True)
        kmk_group['bldg_name_en']=kmk_group['bldg_name_en'].replace({' MANSION': ' MANS'}, regex=True)
        kmk_group['est_name_en']=kmk_group['est_name_en'].replace({' MANS': ' MANSION'}, regex=True)
        kmk_group['bldg_name_en']=kmk_group['bldg_name_en'].replace({' MANS': ' MANSION'}, regex=True)
        kmk_group['est_name_en']=kmk_group['est_name_en'].replace({' HTS': ' HEIGHTS'}, regex=True)
        kmk_group['bldg_name_en']=kmk_group['bldg_name_en'].replace({' HTS': ' HEIGHTS'}, regex=True)

        #Add District Chinese:
        dist_df=district_df.set_index('d_code')
        dist_dict=dist_df.to_dict()
        kmk_group['code_chinese']=kmk_group['d_code']
        kmk_group=kmk_group.replace(dist_dict)



        ## Scrutinize instr_date and add bldg_age
        kmk_group=kmk_group[(kmk_group['instr_date']!='2917-10-13') & (kmk_group['instr_date']!='2301-09-10') & (kmk_group['instr_date']!='9999-12-12')
                           & (kmk_group['op_date']!='A610823') &(kmk_group['op_date']!='PRE-194')
                            & (kmk_group['op_date']!='197/01') & (kmk_group['op_date']!='200/12')
                            & (kmk_group['op_date']!='199/10')]

            #kmk_group.at[8097,'instr_date']='2017-10-13'
        kmk_group['instr_date'] = pd.to_datetime(kmk_group['instr_date'], format="%Y-%m-%d")
        kmk_group=kmk_group[kmk_group['instr_date']<datetime.datetime.now()]


        kmk_group['op_date']=kmk_group['op_date'].replace('blank', '9999')
        kmk_group['op_year']=kmk_group['op_date'].str[:4].astype(int)
        kmk_group['bldg_age']= datetime.datetime.now().year - kmk_group['op_year']
        #bldgAge

        kmk_group['bldg_name_ench']=kmk_group['bldg_name_en']+kmk_group['bldg_name_ch']
        kmk_group['st_no_start']=kmk_group['st_no_start'].astype(str).str.split('.',expand=True)[0] #convert and make sure no decimals left after astype



        #try:
        kmk_group['st_address_en']=kmk_group['st_no_start']+'-'+kmk_group['st_no_end']+' '+kmk_group['st_name_en']
        kmk_group['st_address_ch']=kmk_group['st_no_start']+'-'+kmk_group['st_no_end']+' '+kmk_group['st_name_ch']
        kmk_group['st_addr_google']=kmk_group['st_no_start']+'-'+kmk_group['st_no_end']+' '+kmk_group['st_name_en']+',Hong Kong'
        #except:
        #    print(kmk_group.info())

        #supplement bldg/est name for google if no street number
        no_st_number_df= kmk_group[kmk_group['st_no_start']==' '] #if start number==" ", no end number
        #print(no_st_number_df)
        no_st_number_df['st_address_en']=no_st_number_df['st_name_en']
        no_st_number_df['st_address_ch']=no_st_number_df['st_name_ch']
        no_st_hv_bldg_df=no_st_number_df[no_st_number_df['bldg_name_en']!='blank']
        #print(no_st_number_df['bldg_name_en'])
        if len(no_st_hv_bldg_df)>0:
            no_st_hv_bldg_df['st_addr_google']=no_st_hv_bldg_df['bldg_name_en']+','+no_st_hv_bldg_df['st_name_en']+',Hong Kong'
            kmk_group.update(no_st_hv_bldg_df)
            print(kmk_group)

        no_st_hv_est_df=no_st_number_df[(no_st_number_df['bldg_name_en']=='blank') & (no_st_number_df['est_name_en']!='blank')]
        if len(no_st_hv_est_df)>0:
            no_st_hv_est_df['st_addr_google']=no_st_hv_est_df['est_name_en']+','+no_st_hv_est_df['st_name_en']+',Hong Kong'
            print(no_st_hv_est_df)
            kmk_group.update(no_st_hv_est_df)
            print(kmk_group)

        kmk_group['st_address']=kmk_group['st_address_en']+"|"+kmk_group['st_address_ch']

        kmk_group.replace(r'(-blank)(\s\w)', r'\2', regex=True, inplace= True) #\2 capture the second bracket

        for d_ind, d_row in kmk_group[:].iterrows():
            #if type(d_row['bldg_name_ch'])==float and d_row['bldg_name_en']!='blank':  # Some development has only English name, replace chinese with the english
            #    #print(d_row,d_row['bldg_name_en'])
            #    d_row['bldg_name_ch']=d_row['bldg_name_en']

            if d_row['bldg_name_en']!='blank':
                if d_row['est_name_en']!='blank':
                    if d_row['block']!='blank':
                        if d_row['phase']!='blank' and d_row['est_name_en']!='LOHAS PARK':
                            kmk_group.at[d_ind,'ch_address']=d_row['est_name_ch']+','+d_row['phase']+'期'+','+d_row['block']+'座'+'('+str(d_row['bldg_name_ch'])+')'+','+d_row['st_address_ch']
                            kmk_group.at[d_ind,'en_address']=d_row['bldg_name_en']+'('+'Block '+d_row['block']+','+'Phase '+d_row['phase']+','+str(d_row['est_name_en'])+')'+','+d_row['st_address_en']
                            kmk_group.at[d_ind,'ch_addr_google']=str(d_row['bldg_name_ch'])+','+d_row['est_name_ch']+','+d_row['st_name_ch']+',香港'
                            kmk_group.at[d_ind,'en_addr_google']=str(d_row['bldg_name_en'])+','+d_row['est_name_en']+','+d_row['st_name_en']+',Hong Kong'
                        elif d_row['phase']!='blank' and d_row['est_name_en']=='LOHAS PARK':
                            print('special LOHAS PARK')
                            kmk_group.at[d_ind,'ch_address']=d_row['est_name_ch']+','+d_row['phase']+'期'+','+d_row['block']+'座'+'('+str(d_row['bldg_name_ch'])+')'+','+d_row['st_address_ch']
                            kmk_group.at[d_ind,'en_address']=d_row['bldg_name_en']+'('+'Block '+d_row['block']+','+'Phase '+d_row['phase']+','+str(d_row['est_name_en'])+')'+','+d_row['st_address_en']
                            kmk_group.at[d_ind,'ch_addr_google']=str(d_row['bldg_name_ch'])+','+d_row['est_name_ch']+','+d_row['st_name_ch']+',香港'
                            kmk_group.at[d_ind,'en_addr_google']=str(d_row['bldg_name_en']).split( '(' )[0]+','+d_row['est_name_en']+','+d_row['st_name_en']+',Hong Kong'
                        elif d_row['phase']=='blank':
                            kmk_group.at[d_ind,'ch_address']=d_row['est_name_ch']+','+d_row['block']+'座'+'('+str(d_row['bldg_name_ch'])+')'+','+d_row['st_name_ch']
                            kmk_group.at[d_ind,'en_address']=d_row['bldg_name_en']+'('+'Block '+d_row['block']+','+str(d_row['est_name_en'])+"),"+d_row['st_name_en']
                            kmk_group.at[d_ind,'ch_addr_google']=str(d_row['bldg_name_ch'])+','+d_row['est_name_ch']+','+d_row['st_name_ch']+',香港'
                            kmk_group.at[d_ind,'en_addr_google']=str(d_row['bldg_name_en'])+','+d_row['est_name_en']+','+d_row['st_name_en']+',Hong Kong'
                    elif d_row['block']=='blank':
                        if d_row['phase']!='blank':
                            kmk_group.at[d_ind,'ch_address']=d_row['est_name_ch']+','+d_row['phase']+'期'+'('+str(d_row['bldg_name_ch'])+')'+','+d_row['st_name_ch']
                            kmk_group.at[d_ind,'en_address']=d_row['bldg_name_en']+'('+'Phase '+d_row['phase']+','+str(d_row['est_name_en'])+"),"+d_row['st_name_en']
                            kmk_group.at[d_ind,'ch_addr_google']=str(d_row['bldg_name_ch'])+','+d_row['est_name_ch']+','+d_row['st_name_ch']+',香港'
                            kmk_group.at[d_ind,'en_addr_google']=str(d_row['bldg_name_en'])+','+d_row['est_name_en']+','+d_row['st_name_en']+',Hong Kong'
                        elif d_row['phase']=='blank':
                            kmk_group.at[d_ind,'ch_address']=d_row['est_name_ch']+'('+str(d_row['bldg_name_ch'])+')'+','+d_row['st_name_ch']
                            kmk_group.at[d_ind,'en_address']=d_row['bldg_name_en']+'('+str(d_row['est_name_en'])+"),"+d_row['st_name_en']
                            kmk_group.at[d_ind,'ch_addr_google']=str(d_row['bldg_name_ch'])+','+d_row['est_name_ch']+','+d_row['st_name_ch']+',香港'
                            kmk_group.at[d_ind,'en_addr_google']=str(d_row['bldg_name_en'])+','+d_row['est_name_en']+','+d_row['st_name_en']+',Hong Kong'

                elif d_row['est_name_en']=='blank':
                    if d_row['block']!='blank':
                        if d_row['phase']!='blank':
                            kmk_group.at[d_ind,'ch_address']=d_row['bldg_name_ch']+','+d_row['phase']+'期'+','+d_row['block']+'座'+','+d_row['st_address_ch']
                            kmk_group.at[d_ind,'en_address']=d_row['bldg_name_en']+',Block '+d_row['block']+','+'Phase '+d_row['phase']+','+d_row['st_address_en']
                            kmk_group.at[d_ind,'ch_addr_google']=str(d_row['bldg_name_ch'])+','+d_row['block']+'座,'+d_row['st_name_ch']+',香港'
                            kmk_group.at[d_ind,'en_addr_google']=str(d_row['bldg_name_en'])+',Block '+d_row['block']+','+d_row['st_name_en']+',Hong Kong'
                        elif d_row['phase']=='blank':
                            kmk_group.at[d_ind,'ch_address']=d_row['bldg_name_ch']+','+d_row['st_name_ch']
                            kmk_group.at[d_ind,'en_address']=d_row['bldg_name_en']+','+d_row['st_name_en']
                            kmk_group.at[d_ind,'ch_addr_google']=str(d_row['bldg_name_ch'])+','+d_row['block']+'座,'+d_row['st_name_ch']+',香港'
                            kmk_group.at[d_ind,'en_addr_google']=str(d_row['bldg_name_en'])+',Block '+d_row['block']+','+d_row['st_name_en']+',Hong Kong'
                    elif d_row['block']=='blank':
                        if d_row['phase']!='blank':
                            kmk_group.at[d_ind,'ch_address']=d_row['bldg_name_ch']+','+d_row['phase']+'期'+','+d_row['st_name_ch']
                            kmk_group.at[d_ind,'en_address']=d_row['bldg_name_en']+',(Phase '+d_row['phase']+"),"+d_row['st_name_en']
                            kmk_group.at[d_ind,'ch_addr_google']=str(d_row['bldg_name_ch'])+','+d_row['phase']+'期,'+d_row['st_name_ch']+',香港'
                            kmk_group.at[d_ind,'en_addr_google']=str(d_row['bldg_name_en'])+',Phase'+d_row['phase']+','+d_row['st_name_en']+',Hong Kong'
                        elif d_row['phase']=='blank':
                            kmk_group.at[d_ind,'ch_address']=d_row['bldg_name_ch']+','+d_row['st_name_ch']
                            kmk_group.at[d_ind,'en_address']=d_row['bldg_name_en']+','+d_row['st_name_en']
                            kmk_group.at[d_ind,'ch_addr_google']=str(d_row['bldg_name_ch'])+','+d_row['st_name_ch']+',香港'
                            kmk_group.at[d_ind,'en_addr_google']=str(d_row['bldg_name_en'])+','+d_row['st_name_en']+',Hong Kong'


            elif d_row['bldg_name_en']=='blank':
                if d_row['est_name_en']!='blank':
                    if d_row['block']!='blank':
                        if d_row['phase']!='blank':
                            kmk_group.at[d_ind,'ch_address']= str(d_row['est_name_ch'])+d_row['phase']+"期"+','+d_row['block']+"座"+','+d_row['st_name_ch']
                            kmk_group.at[d_ind,'en_address']= d_row['est_name_en']+"(Phase "+d_row['phase']+"),"+"Block "+d_row['block']+","+d_row['st_name_en']
                            kmk_group.at[d_ind,'ch_addr_google']=str(d_row['est_name_ch'])+','+d_row['block']+'座'+','+d_row['st_name_ch']+',香港'
                            kmk_group.at[d_ind,'en_addr_google']=str(d_row['est_name_en'])+',Block '+d_row['block']+','+d_row['st_name_en']+',Hong Kong'
                        elif d_row['phase']=='blank':
                            kmk_group.at[d_ind,'ch_address']= str(d_row['est_name_ch'])+d_row['block']+"座"+','+d_row['st_name_ch']
                            kmk_group.at[d_ind,'en_address']= d_row['est_name_en']+"(Block "+d_row['block']+"),"+d_row['st_name_en']
                            kmk_group.at[d_ind,'ch_addr_google']=str(d_row['est_name_ch'])+','+d_row['block']+'座'+','+d_row['st_name_ch']+',香港'
                            kmk_group.at[d_ind,'en_addr_google']=str(d_row['est_name_en'])+',Block '+d_row['block']+','+d_row['st_name_en']+',Hong Kong'
                    elif d_row['block']=='blank':
                        if d_row['phase']!='blank':
                            kmk_group.at[d_ind,'ch_address']= str(d_row['est_name_ch'])+d_row['phase']+"期"+','+d_row['st_name_ch']
                            kmk_group.at[d_ind,'en_address']= d_row['est_name_en']+"(Phase "+d_row['phase']+"),"+d_row['st_name_en']
                            kmk_group.at[d_ind,'ch_addr_google']=str(d_row['est_name_ch'])+','+d_row['phase']+','+','+d_row['st_name_ch']+',香港'
                            kmk_group.at[d_ind,'en_addr_google']=str(d_row['est_name_en'])+','+d_row['phase']+','+d_row['st_name_en']+',Hong Kong'
                        elif d_row['phase']=='blank':
                            kmk_group.at[d_ind,'ch_address']=str(d_row['est_name_ch'])+','+d_row['st_name_ch']
                            kmk_group.at[d_ind,'en_address']=d_row['est_name_en']+','+d_row['st_name_en']
                            kmk_group.at[d_ind,'ch_addr_google']=str(d_row['est_name_ch'])+','+d_row['st_name_ch']+',香港'
                            kmk_group.at[d_ind,'en_addr_google']=d_row['est_name_en']+','+d_row['st_name_en']+',Hong Kong'

                elif d_row['est_name_en']=='blank':
                    if d_row['block']!='blank':
                        if d_row['phase']!='blank':
                            kmk_group.at[d_ind,'ch_address']='第'+d_row['phase']+'期'+','+'第'+d_row['block']+'座'+','+d_row['st_address_ch']
                            kmk_group.at[d_ind,'en_address']='Block '+d_row['block']+','+'Phase '+d_row['phase']+','+d_row['st_address_en']
                            kmk_group.at[d_ind,'ch_addr_google']=d_row['phase']+'期'+','+d_row['block']+'座,'+d_row['st_name_ch']+',香港'
                            kmk_group.at[d_ind,'en_addr_google']='Block '+d_row['block']+','+'Phase '+d_row['phase']+','+d_row['st_name_en']+',Hong Kong'
                        elif d_row['phase']=='blank':
                            kmk_group.at[d_ind,'ch_address']='第'+d_row['block']+'座'+','+d_row['st_address_ch']
                            kmk_group.at[d_ind,'en_address']='Block '+d_row['block']+','+d_row['st_address_en']
                            kmk_group.at[d_ind,'ch_addr_google']=d_row['block']+'座,'+d_row['st_name_ch']+',香港'
                            kmk_group.at[d_ind,'en_addr_google']='Block '+d_row['block']+','+d_row['st_name_en']+',Hong Kong'
                    elif d_row['block']=='blank':
                        if d_row['phase']!='blank':
                            kmk_group.at[d_ind,'ch_address']='第'+d_row['phase']+'期'+','+d_row['st_address_ch']
                            kmk_group.at[d_ind,'en_address']='Phase '+d_row['phase']+','+d_row['st_address_en']
                            kmk_group.at[d_ind,'ch_addr_google']=d_row['phase']+'期,'+d_row['st_name_ch']+',香港'
                            kmk_group.at[d_ind,'en_addr_google']='Phase '+d_row['phase']+','+d_row['st_name_en']+',Hong Kong'
                        elif d_row['phase']=='blank':
                            kmk_group.at[d_ind,'ch_address']=d_row['st_address_ch']
                            kmk_group.at[d_ind,'en_address']=d_row['st_address_en']
                            kmk_group.at[d_ind,'ch_addr_google']=d_row['st_name_ch']+',香港'
                            kmk_group.at[d_ind,'en_addr_google']=d_row['st_name_en']+',Hong Kong'

            else:
                kmk_group.at[d_ind,'ch_address']=d_row['st_address_ch']
                kmk_group.at[d_ind,'en_address']=d_row['st_address_en']
                kmk_group.at[d_ind,'ch_addr_google']=d_row['st_name_ch']+',香港'
                kmk_group.at[d_ind,'en_addr_google']=d_row['st_name_en']+',Hong Kong'


            kmk_group.at[d_ind,'final_address']=kmk_group.at[d_ind,'en_address']+"|"+kmk_group.at[d_ind,'ch_address']


        address_list=kmk_group['final_address'].unique()
        #print('179 SAI YEE ST|179 洗衣街' in address_list)
        kmk_removedInactive_df=kmk_group.copy()
        for bldg_merged in address_list:
            bldg_df=kmk_group[(kmk_group['final_address']==bldg_merged)]
            # Remove bldg with less than 2 transaction in the past 5 years to avoid domination by if non-market data
            ## This substantially reduced number of bldg(some bldg address may actually changed so has large number)
            if len(bldg_df[bldg_df['instr_date']>=(datetime.datetime.now()-datetime.timedelta(5*365))])<=1:
                #print(bldg_merged)
                kmk_removedInactive_df.drop(bldg_df.index,inplace=True)




        print(district,len(kmk_removedInactive_df['final_address'].unique()))  #,datetime.datetime.now())
        bldg_no+=len(kmk_removedInactive_df['final_address'].unique())
        #sorted(kmk_group['final_address'].unique())

        kmk_removedInactive_df.to_csv(urlTo_valSystem+ '/raw_data/{}_addressed_df.csv'.format(district))
    bldg_no


    # In[ ]:


    dist_counter2=0
    for district in district_df['d_code'][dist_counter2:]:
        print(district, dist_counter2)
        dist_counter2+=1

        url_dup = urlTo_valSystem+ '/raw_data/{}_addressed_df.csv'.format(district)
        kmk_dup= pd.read_csv(url_dup, keep_default_na=False, index_col =0)

        #temp
        #kmk_dup.replace(r'^\s*$', 'blank', regex=True, inplace= True)

        #remove duplicates
        kmk_dup['final_address_consid']= kmk_dup['final_address']+'|'+kmk_dup['floor'].astype(str)+'|'+kmk_dup['unit'].astype(str) +'consid'+kmk_dup['consid'].astype(str)
        #print(kmk_dup.iloc[0])
        #display(kmk_dup)
        kmk_dup_drop = kmk_dup.drop_duplicates(subset ='final_address_consid', keep = 'first')

        print(len(kmk_dup_drop))

        #remove zero npsf and zero nfa
        kmk_rem0 = kmk_dup_drop.copy()
        kmk_removed0 = kmk_rem0.drop(kmk_rem0[kmk_rem0['npsf']==0].index)  #kmk_rem0[kmk_rem0[['npsf']].all(axis='columns')]
        kmk_removed0 = kmk_removed0.drop(kmk_removed0[kmk_removed0['nfa']==0].index)

        #remove outliers
        kmk_out = kmk_removed0.copy()
        kmk_out['instr_date'] = pd.to_datetime(kmk_out['instr_date'], format="%Y-%m-%d")
        kmk_out['floor_copy'] = kmk_out['floor']
        kmk_out['floor'] = pd.to_numeric(kmk_out['floor'], errors='coerce')
        kmk_out['floor']=kmk_out['floor'].replace(np.nan,0)

        bldg_list =  kmk_out['final_address'].unique()
        #bldg_list =['洋松街WING SHUN BLDG永信大樓|Phase|Block']
        filt_st_yr = list(range(1997,2021))
        for year in filt_st_yr:
            print('up to ' + str(year))
            for bldg in bldg_list:
                filt_subj = kmk_out[(kmk_out['final_address'] == bldg) & (kmk_out['instr_date']>= datetime.datetime(year,1,1)) & (kmk_out['instr_date']<= datetime.datetime(year,7,1))]
                if len(filt_subj)<10:
                    filt_subj = kmk_out[(kmk_out['final_address'] == bldg) & (kmk_out['instr_date']>= datetime.datetime(year,1,1)) & (kmk_out['instr_date']<= datetime.datetime(year+1,1,1))]
                    if len(filt_subj)<10:
                        filt_subj = kmk_out[(kmk_out['final_address'] == bldg) & (kmk_out['instr_date']>= datetime.datetime(year,1,1)) & (kmk_out['instr_date']<= datetime.datetime(year+1,7,1))]
                        if len(filt_subj)<10:
                            filt_subj = kmk_out[(kmk_out['final_address'] == bldg) & (kmk_out['instr_date']>= datetime.datetime(year,1,1)) & (kmk_out['instr_date']<= datetime.datetime(year+2,1,1))]
                            if len(filt_subj)<10:
                                filt_subj = kmk_out[(kmk_out['final_address'] == bldg) & (kmk_out['instr_date']>= datetime.datetime(year,1,1)) & (kmk_out['instr_date']<= datetime.datetime(year+2,7,1))]
                #print(filt_subj)
                #filter outlier part:
                if len(filt_subj) ==0:
                    #print('x')
                    continue
                q25, q75, median = np.percentile(filt_subj['npsf'], 25), np.percentile(filt_subj['npsf'], 75), np.percentile(filt_subj['npsf'], 50)
                iqr = q75 - q25
                if len(filt_subj) <20:
                    down, up = 0.6, 0.9
                else:
                    down, up = 1, 1.5 # when transaction is more, iqr tend to be narrower/ price range spread wider
                lower, upper = q25 - iqr*down, q75 + iqr*up
                modi_lower, modi_upper = min(lower, median*(1-0.4*down)), max(upper, median*(1+0.4*up)) # calculate the outlier cutoff
                #print(modi_lower, modi_upper)
                outliers_index = [x[0] for x in filt_subj.iterrows() if x[1].loc['npsf'] < modi_lower or x[1].loc['npsf'] > modi_upper] # identify outliers
                kmk_out = kmk_out.drop(outliers_index)
                if len (outliers_index) > 4:
                    print(filt_subj[['final_address','npsf']])
                    print(median, outliers_index)
        kmk_out=kmk_out[kmk_out['npsf']>3000] #To filter away those thinly transacted building dominated with below market transaction
        kmk_out.to_csv(urlTo_valSystem+ '/raw_data/{}_filtered_varIQR.csv'.format(district))


    # #Remove Transactions of same unit

    # In[ ]:


    dist_counter3=0

    for district in district_df['d_code'][dist_counter3:]:
        print(district, dist_counter3)
        dist_counter3+=1

        url_out_removed = urlTo_valSystem+ '/raw_data/{}_filtered_varIQR.csv'.format(district) # Use filtered data set
        kmk_specu= pd.read_csv(url_out_removed, keep_default_na=False, index_col =0)

        kmk_specu['instr_date'] = pd.to_datetime(kmk_specu['instr_date'])


        #Remove all same unit transaction within half yr
        check_int=183
        check_st =datetime.datetime(1997,1,1)
        kmk_specu_removed= kmk_specu.copy()
        for day_increment in range(((datetime.datetime.now()-datetime.timedelta(check_int))-check_st).days): #to construct day range list from today-check_interval
            check_date = check_st + datetime.timedelta(day_increment)
            check_period_df = kmk_specu[(kmk_specu['instr_date']>=check_date) & (kmk_specu['instr_date']<=check_date+datetime.timedelta(check_int))]
            part_df = check_period_df[check_period_df.duplicated(subset=['subject_pty','nat_instr'],keep='last')] #find out the rows to be removed
            #first_df = check_period_df[check_period_df.duplicated(subset=['subject_pty','nat_instr'],keep='first')]
            #all_df = check_period_df[check_period_df.duplicated(subset=['subject_pty','nat_instr'],keep=False)]
            #print (len(check_period_df))
            #print (len(part_df))
            #print(part_df,first_df,all_df)
            #print(len(kmk_specu_proxy))
            kmk_specu_removed=kmk_specu_removed.drop(part_df.index, errors= 'ignore') # remove the rows to be kept to find rows to be deleted
            #print(len(kmk_specu_proxy))

        #Remove same property transaction within a year if npsf change by 15%+
        check_inter = 365
        remove_list = []
        for index, row in kmk_specu_removed.iterrows(): #.iloc[list(range(48000,49000)),:]
            #print(row)
            target_trans = row.loc['subject_pty']
            trans_date = row.loc['instr_date']
            trans_consid = row.loc['consid']
            trans_nat = row.loc['nat_instr']
            search_period_df = kmk_specu_removed[(kmk_specu_removed['instr_date']>=trans_date) & (kmk_specu_removed['instr_date']<= trans_date+datetime.timedelta(check_inter))
            & (kmk_specu_removed['subject_pty']==target_trans) & (kmk_specu_removed['nat_instr']==trans_nat)]
            if len(search_period_df)>1:
                #print(search_period_df)
                sec_consid = search_period_df['consid'].iloc[1]
                sec_date =search_period_df['instr_date'].iloc[1]
                price_change=sec_consid/trans_consid
                if abs(price_change/((sec_date-trans_date).days/365)) >0.15:
                    remove_list.append(index)
        kmk_specu_removed2=kmk_specu_removed.drop(remove_list)


        kmk_specu_removed2.to_csv(urlTo_valSystem+ '/raw_data/{}_filtered_IQR&specu.csv'.format(district))


    # In[ ]:


    # Build Anciliary Area CSV
    dist_counter2=0
    """['BW:','BW','BAL:','BAL','F/R:','FR:','F/R','RF:','RF',
    'UTILITY PLATFORM:','UP:','UTILITY PLATFORM','A/C PLATFORM:','A/C PLATFORM',
    'GDN:','GDN',' RM:',' RM','YD:',' YD',
    'CPS ','DUPLEX ','BELOW MARKET VALUE'
    ]"""
    anci_area_list=['bay_window','has_BW','balcony','has_BAL','F/R','FR','has_FR','roof','has_roof',
                    'UTILITY_PLATFORM','UP','has_UP','ac_platform','has_acPlatfrom','plant_room','has_RM',
                    'garden','has_GDN','yard','has_YD',
                    'has_CPS','is_duplex','below_market_trans'
                   ]
    for district in district_df['d_code'][dist_counter2:]:
        #district='HCW'
        print(district, dist_counter2)
        dist_counter2+=1

        url_addr = urlTo_valSystem+ '/raw_data/{}_filtered_IQR&specu.csv'.format(district)
        df_wAddr= pd.read_csv(url_addr, keep_default_na=False, index_col =0)

        anci_area_df=df_wAddr[['final_address','floor','unit','instr_date','remark1','remark2']]
        anci_area_df['remark_all']=anci_area_df['remark1']+','+anci_area_df['remark2']
        list_no=0
        anci_area_df[anci_area_list[list_no]]=anci_area_df['remark_all'].str.extract('.*BW[:\s](\d+).*')
        #print((anci_area_df[anci_area_df['BW:'].isnull()]))
        #break
        list_no+=1
        anci_area_df[anci_area_list[list_no]]=anci_area_df['remark_all'].str.contains('BW[,\s]')
        list_no+=1
        anci_area_df[anci_area_list[list_no]]=anci_area_df['remark_all'].str.extract('.*BAL:(\d+).*SF')
        list_no+=1
        anci_area_df[anci_area_list[list_no]]=anci_area_df['remark_all'].str.contains('BAL[,\s]')
        list_no+=1
        anci_area_df[anci_area_list[list_no]]=anci_area_df['remark_all'].str.extract('.*F\/R:(\d+).*SF')
        list_no+=1
        anci_area_df[anci_area_list[list_no]]=anci_area_df['remark_all'].str.extract('.*FR:(\d+).*SF')
        list_no+=1
        anci_area_df[anci_area_list[list_no]]=anci_area_df['remark_all'].str.contains('F\/R[,\s]')
        list_no+=1
        anci_area_df[anci_area_list[list_no]]=anci_area_df['remark_all'].str.extract('.*RF:(\d+).*SF')
        list_no+=1
        anci_area_df[anci_area_list[list_no]]=anci_area_df['remark_all'].str.contains('RF[,\s]')
        list_no+=1
        anci_area_df[anci_area_list[list_no]]=anci_area_df['remark_all'].str.extract('.*UTILITY\sPLATFORM:(\d+).*SF')
        list_no+=1
        anci_area_df[anci_area_list[list_no]]=anci_area_df['remark_all'].str.extract('.*UP:(\d+).*SF')
        list_no+=1
        anci_area_df[anci_area_list[list_no]]=anci_area_df['remark_all'].str.contains('UTILITY\sPLATFORM[,\s]')
        list_no+=1
        anci_area_df[anci_area_list[list_no]]=anci_area_df['remark_all'].str.extract('.*A\/C\sPLATFORM:(\d+).*SF')
        list_no+=1
        anci_area_df[anci_area_list[list_no]]=anci_area_df['remark_all'].str.contains('A\/C\sPLATFORM[,\s]')
        list_no+=1
        anci_area_df[anci_area_list[list_no]]=anci_area_df['remark_all'].str.extract('.*GDN:(\d+).*SF')
        list_no+=1
        anci_area_df[anci_area_list[list_no]]=anci_area_df['remark_all'].str.contains('GDN[,\s]')
        list_no+=1
        anci_area_df[anci_area_list[list_no]]=anci_area_df['remark_all'].str.extract('.*\sRM:(\d+).*SF')
        list_no+=1
        anci_area_df[anci_area_list[list_no]]=anci_area_df['remark_all'].str.contains('\sRM[,\s]')
        list_no+=1
        anci_area_df[anci_area_list[list_no]]=anci_area_df['remark_all'].str.extract('.*YD:(\d+).*SF')
        list_no+=1
        anci_area_df[anci_area_list[list_no]]=anci_area_df['remark_all'].str.contains('YD[,\s]')
        list_no+=1
        anci_area_df[anci_area_list[list_no]]=anci_area_df['remark_all'].str.contains('CPS[,\s]')
        list_no+=1
        anci_area_df[anci_area_list[list_no]]=anci_area_df['remark_all'].str.contains('DUPLEX[,\s]')
        list_no+=1
        anci_area_df[anci_area_list[list_no]]=anci_area_df['remark_all'].str.contains('BELOW\sMARKET\sVALUE[,\s]')
        list_no+=1

        ##Combine same features: flat roof and utility platform
        extr_a=anci_area_df[['F/R']]
        extr_a['flat_roof']=extr_a['F/R']
        extr_b=anci_area_df[['FR']]
        extr_b['flat_roof']=extr_b['FR']
        flatRoof_df=extr_b.combine_first(extr_a)
        anci_area_df['flat_roof']=flatRoof_df['flat_roof']
        extr_a=anci_area_df[['UTILITY_PLATFORM']]
        extr_a['utility_platform']=extr_a['UTILITY_PLATFORM']
        extr_b=anci_area_df[['UP']]
        extr_b['utility_platform']=extr_b['UP']
        utilityPlatform_df=extr_b.combine_first(extr_a)
        anci_area_df['utility_platform']=utilityPlatform_df['utility_platform']


        ##remove duplicated unit info
        anci_area_df['final_address_floor_unit']= anci_area_df['final_address']+'_'+anci_area_df['floor'].astype(str)+'_'+anci_area_df['unit']
        #anci_area_df = anci_area_df.groupby('final_address_floor_unit').agg('last').reset_index()  #Keep latest transaction remark info
        #display(anci_area_df)
        #print(anci_area_df['final_address_floor_unit'])

        anci_area_df.to_csv(urlTo_valSystem+ '/raw_data/{}_anci_area_table.csv'.format(district))


    # In[3]:


    other_area_list_df=pd.DataFrame(columns=['other_area_type','其他面積類別'])
    other_area_eng_list=['Bay Window','Flat Roof','Roof','AC Platform','Internal Plant Room','Garden','Yard']
    other_area_chi_list=['窗台','平台','天台','冷氣機平台','室內機房','花園','庭院']
    other_area_code_list=['bay_window','flat_roof','roof','ac_platform',
                    'plant_room','garden','yard']
    other_area_list_df['other_area_type']=other_area_eng_list
    other_area_list_df['其他面積類別']=other_area_chi_list
    other_area_list_df['code_name']=other_area_code_list
    other_area_list_df['discount_factor']=[4,6,10,8,6,10,8]
    other_area_list_df.to_csv(urlTo_valSystem+ '/raw_data/other_area_factor_table.csv')


    # ## Add other area to the info

    # In[ ]:


    dist_counter4=0
    for district in district_df['d_code'][dist_counter4:]:
        #district='KMK'
        #district="HQB"
        print(district,dist_counter4)
        dist_counter4+=1
        url_anciArea = urlTo_valSystem+ '/raw_data/{}_anci_area_table.csv'.format(district) # Use filtered data set
        anciArea_df= pd.read_csv(url_anciArea, keep_default_na=False, index_col =0)
        anciArea_df= anciArea_df.drop(columns=['F/R','FR','UTILITY_PLATFORM','UP'])
        #anciArea_df= anciArea_df.drop(columns=['F/R:','FR:','UTILITY_PLATFORM:','UP:']) old version

        url_filterSpecu = urlTo_valSystem+ '/raw_data/{}_filtered_IQR&specu.csv'.format(district) # Use filtered data set
        kmk_time= pd.read_csv(url_filterSpecu, keep_default_na=False, index_col =0)
        kmk_time = kmk_time.drop(kmk_time[kmk_time['nfa']==0].index)
        kmk_time['other_area']='No_other_area'

        for indexTime, rowTime in kmk_time.iterrows():
            #print(rowTime)
            row_addr_floor_unit= rowTime['final_address']+'_'+str(rowTime['floor'])+'_'+rowTime['unit']
            #print(row_addr_floor_unit)
            #print(anciArea_df.loc[6390]['final_address_floor_unit'])
            match_trans_anciArea_df= anciArea_df[anciArea_df['final_address_floor_unit']==row_addr_floor_unit]
            #print(match_trans_anciArea_df)
            match_trans_anciArea_sr=match_trans_anciArea_df.iloc[-1].dropna()
            match_trans_anciArea_sr= match_trans_anciArea_sr[match_trans_anciArea_sr!=False]
            if len(match_trans_anciArea_sr)>8:
                strim_sr=match_trans_anciArea_sr.loc[other_area_code_list]  # Selected items only, may include more in future
                kmk_time.at[indexTime,'other_area']=strim_sr.to_dict()
            else:
                #print(match_trans_anciArea_sr)
                continue
            #print(strim_sr.to_dict())

        #Adjust transaction with CPS (use subj npsf, website use bldg avg npsf, best is to individul assess)
        anci_area_df=kmk_time[['final_address','floor','unit','instr_date','remark1','remark2']]
        anci_area_df['remark_all']=anci_area_df['remark1']+','+anci_area_df['remark2']
        anci_area_df['with_CPS']=anci_area_df['remark_all'].str.contains('CPS[,\s]')

        kmk_time['with_CPS']=anci_area_df['with_CPS']
        kmk_time['original_consid']=kmk_time['consid']
        kmk_time['original_npsf']=kmk_time['npsf']

        df_wCPS= kmk_time[kmk_time['with_CPS']==True]
        df_wCPS['consid']=df_wCPS['original_consid']-(df_wCPS['original_npsf']*100)/1000000 #estimated that CPS value to be ~100*npsf
        for indexCPS, rowCPS in df_wCPS.iterrows():
            #kmk_time.at[indexCPS,'est_CPS_value']= rowCPS.loc['original_npsf']*100/1000000
            kmk_time.at[indexCPS,'consid']= rowCPS.loc['consid']
            kmk_time.at[indexCPS,'npsf']= rowCPS.loc['consid']*1000000/rowCPS.loc['nfa']

        #kmk_specu_removed2[kmk_specu_removed2['has_CPS']==True][['final_address','npsf','original_npsf']]

        kmk_time.to_csv(urlTo_valSystem+ '/raw_data/{}_filtered_wOtherArea.csv'.format(district))

    kmk_time.columns


    # # Time Index Development

    # In[ ]:


    # Time Index Development
    # Match Transaction Pairs
    dist_counter4=0
    for district in district_df['d_code'][:]:
        print(district,dist_counter4)
        dist_counter4+=1

        url_filtered = urlTo_valSystem+ '/raw_data/{}_filtered_IQR&specu.csv'.format(district) # Use filtered data set
        kmk_index= pd.read_csv(url_filtered, keep_default_na=False, index_col =0)
        kmk_index['instr_date'] = pd.to_datetime(kmk_index['instr_date'], format="%Y-%m-%d")
        kmk_pair_df = kmk_index[['subject_pty','instr_date','consid','nfa','gfa']]
        kmk_pair_df['2nd_sales_date']= 'blank'
        kmk_pair_df['2nd_sales_consid']= 'blank'

        kmk_copy = kmk_pair_df.copy()
        kmk_pair_df.replace(0,np.nan, inplace=True)
        #display(kmk_pair_df)

        unit_arr = kmk_pair_df['subject_pty'].unique()
        #len(unit_arr)
        for item in unit_arr:
            df_be_checked = kmk_pair_df[kmk_pair_df['subject_pty']==item]
            if len(df_be_checked)>1:
                for index, row in df_be_checked.iterrows():
                    #print(len(df_be_checked))
                    for i in df_be_checked.index:
                        if (row.loc['instr_date']> df_be_checked.at[i,'instr_date']) and (row.loc['consid'] != df_be_checked.at[i,'consid']) and (kmk_pair_df.loc[index,'2nd_sales_date'] =='blank') and ((row.loc['nfa']== df_be_checked.loc[i,'nfa']) or (row.loc['gfa']== df_be_checked.loc[i,'gfa'])):
                            #print(row, 'and ', df_be_checked.iloc[i,3])
                            #print (kmk_pair_df.iloc[df_be_checked.iloc[i].name,1], 'and ', row.iloc[1])
                            kmk_copy.at[i,'2nd_sales_index'] = index
                            kmk_copy.at[i,'2nd_sales_date'] = row.loc['instr_date']
                            kmk_copy.at[i,'2nd_sales_consid'] = row.loc['consid']

        #display(kmk_copy)
        kmk_copy['op_date'] = kmk_index['op_date']
        for index in kmk_copy['op_date'].index:
            if kmk_copy.loc[index,'op_date'][-2:]=='00':
                kmk_copy.loc[index,'op_date'] = kmk_copy.loc[index,'op_date'][:-2]+'01'
                #print(kmk_copy.loc[index,'op_date'])
        kmk_copy.to_csv(urlTo_valSystem+ '/raw_data/{}_index_paired_filtered_indexed.csv'.format(district))


    # In[ ]:


    # Index Construction 1: Build District Index
    for district in district_df['d_code'][:]:
        print(district)

        url_filtered = urlTo_valSystem+ '/raw_data/{}_index_paired_filtered_indexed.csv'.format(district) # Use filtered data set
        kmk_copy= pd.read_csv(url_filtered, keep_default_na=False, index_col =0)



        #if district in ['NPSK']:
            #print('Need suppl')
            #suppl_url= urlTo_valSystem+ '/raw_data/NT_index_paired_filtered_indexed.csv'
            #suppl_df = pd.read_csv(suppl_url, keep_default_na=False, index_col =0)
            #kmk_copy=kmk_copy.append(suppl_df, ignore_index=True[u'uuuu[uuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuu]]

        kmk_copy = kmk_copy[(kmk_copy['op_date']!='blank') & (kmk_copy['op_date']!='1988//1')& (kmk_copy['op_date']!='1988//0')& (kmk_copy['op_date']!='1999/99')]

        kmk_copy['op_date']=  pd.to_datetime(kmk_copy['op_date'], format="%Y/%m", errors="coerce")
        kmk_transacted = kmk_copy[kmk_copy['2nd_sales_date']!='blank']
        kmk_transacted=kmk_transacted[kmk_transacted['consid']!=0]
        kmk_transacted['2nd_sales_date'] = pd.to_datetime(kmk_transacted['2nd_sales_date'], format="%Y/%m/%d")
        kmk_transacted['instr_date'] = pd.to_datetime(kmk_transacted['instr_date'], format="%Y/%m/%d")
        kmk_transacted['2nd_sales_consid']= pd.to_numeric(kmk_transacted['2nd_sales_consid'])
        kmk_transacted['hldg_len'] = kmk_transacted['2nd_sales_date']-kmk_transacted['instr_date']
        kmk_2 = kmk_transacted[kmk_transacted['hldg_len']>datetime.timedelta(days=365)] # filter out transactions within 1 year
        for ind in kmk_2['hldg_len'].index.to_list():
            kmk_2.loc[ind,'hldg_len'] = kmk_2.loc[ind, 'hldg_len'].days
        print(kmk_copy)
        kmk_2['inc_pyear']= (kmk_2['2nd_sales_consid']/kmk_2['consid']-1)/kmk_2['hldg_len']*365

        ##Set criteria for data selection for index construction
        st_year =1997 #If there is no transactions for the first input in the lm like KKAI, no index can be deduced.
        next_year=datetime.datetime.now().year+1
        st_date = datetime.datetime(st_year,1,1)
        year_change_max =0.2
        kmk_3 = kmk_2[kmk_2['instr_date']>=st_date] # transaction after st_date
        #kmk_3 = kmk_3[(kmk_3['inc_pyear']<year_change_max)&(kmk_2['inc_pyear']>-year_change_max)] # per year price change less than 20%
        #kmk_3 = kmk_3[ (kmk_3['nfa']> subj_nfa/1.5) & (kmk_3['nfa']< subj_nfa*1.5)] # size choice
        #kmk_3 = kmk_3[kmk_3['op_date']>st_date] #age selection as proxy for price range control

        ## Index Period set to by Month
        for year in range(st_year,next_year+1):
            for mon in range(1,13):
                year_mon = str(year)+"-"+str(mon)
                kmk_3[year_mon] = 0
        for index, row in kmk_3.iterrows():
            date = row.loc['instr_date']
            year = str(date.year)
            mon = str(date.month)
            tran_mon = year+'-'+mon
            kmk_3.at[index,tran_mon]=-1

        for index2, row2 in kmk_3.iterrows():
            date2 = row2.loc['2nd_sales_date']
            year2 = str(date2.year)
            mon2 = str(date2.month)
            tran_mon2 = year2+'-'+mon2
            kmk_3.at[index2,tran_mon2]=1
            #print(tran_mon)

        #print(kmk_3['2018-10'].to_list())

        ## Computing index
        kmk_3['log_p']= np.log((kmk_3['2nd_sales_consid']/kmk_3['consid']).astype('float64'))
        kmk_3=kmk_3.dropna(subset=['log_p'])
        kmk_3['p_change']= ((kmk_3['2nd_sales_consid']/kmk_3['consid']).astype('float64'))

        ## Regressing
        lm = LinearRegression(fit_intercept=False)
        data_end = len(kmk_3.columns)-2
        train_df= kmk_3.iloc[:,list(range(12,data_end+1))].dropna()
        X_train = train_df.iloc[:,:-1]
        y_train = train_df.iloc[:,-1]
        #X_train = kmk_3.iloc[:,list(range(12,data_end))] #because 1998 - 2006 Jul data missing, so (12+7)
        #X_train.to_csv(urlTo_valSystem+ '/raw_data/{}_X_train_price_index.csv'.format(district))
        #y_train = kmk_3.iloc[:,data_end]


        #display(kmk_3)
        #print(X_train)
        #print(y_train.to_list())


        #try:
        lm.fit(X_train,y_train)
        #except ValueError:
            #print('value error in:', district)
            #continue

        #print(lm.coef_*100)


        YY = (np.exp(lm.coef_)*100) # set 1997-1-1 level as 100
        y_axis = np.append([100],YY)
        x_axis = []
        for year in range(st_year,next_year+1):
            for mon in range(1,13):
                #for half in [1,16]:
                year_mon = str(year)+"-"+str(mon) #+ '-' + str(half)
                x_axis.append(year_mon)
        #x_axis=x_axis[7:]  #because 1998 - 2006 Jul data missing, so (12+7)
        kmk_price_index= pd.Series(y_axis, index = x_axis)
        kmk_price_index.to_csv(urlTo_valSystem+ '/val_sys/{}_price_index.csv'.format(district))


    # In[ ]:


    # Index Construction 2: Build HK,KLN & NT Index
    three_area=['HK','KLN','NT','full_HK']

    # for full_HK
    #url_filtered = .format(district) # Use filtered data set
    #full_trans_info=pd.DataFrame()
    #for district in district_df['d_code']:
    #    district_trans_info = pd.read_csv(urlTo_valSystem+ '/raw_data/{}_index_paired_filtered_indexed.csv'.format(district),index_col=0)
    #    full_trans_info=full_trans_info.append(district_trans_info,ignore_index=True)

    for area in three_area[:]:
        print(area)
        if area =='HK':
            # for HK
            full_trans_info=pd.DataFrame()
            for district in district_df['d_code'][:20]:
                district_trans_info = pd.read_csv(urlTo_valSystem+ '/raw_data/{}_index_paired_filtered_indexed.csv'.format(district),index_col=0)
                full_trans_info=full_trans_info.append(district_trans_info,ignore_index=True)
        elif area== 'KLN':
            # for KLN
            full_trans_info=pd.DataFrame()
            for district in district_df['d_code'][20:38]:
                district_trans_info = pd.read_csv(urlTo_valSystem+ '/raw_data/{}_index_paired_filtered_indexed.csv'.format(district),index_col=0)
                full_trans_info=full_trans_info.append(district_trans_info,ignore_index=True)
        elif area=='NT':
            # for NT
            full_trans_info=pd.DataFrame()
            for district in district_df['d_code'][38:]:
                district_trans_info = pd.read_csv(urlTo_valSystem+ '/raw_data/{}_index_paired_filtered_indexed.csv'.format(district),index_col=0)
                full_trans_info=full_trans_info.append(district_trans_info,ignore_index=True)
        elif area=='full_HK':
            # for full HK
            full_trans_info=pd.DataFrame()
            for district in district_df['d_code'][:]:
                district_trans_info = pd.read_csv(urlTo_valSystem+ '/raw_data/{}_index_paired_filtered_indexed.csv'.format(district),index_col=0)
                full_trans_info=full_trans_info.append(district_trans_info,ignore_index=True)

        print('finish combine')
        kmk_copy=full_trans_info.copy()

        kmk_copy = kmk_copy[(kmk_copy['op_date']!='blank') & (kmk_copy['op_date']!='1988//1')& (kmk_copy['op_date']!='1988//0')& (kmk_copy['op_date']!='1999/99')]

        kmk_copy['op_date']=  pd.to_datetime(kmk_copy['op_date'], format="%Y/%m", errors="coerce")
        kmk_transacted = kmk_copy[kmk_copy['2nd_sales_date']!='blank']
        kmk_transacted=kmk_transacted[kmk_transacted['consid']!=0]
        kmk_transacted['2nd_sales_date'] = pd.to_datetime(kmk_transacted['2nd_sales_date'], format="%Y/%m/%d")
        kmk_transacted['instr_date'] = pd.to_datetime(kmk_transacted['instr_date'], format="%Y/%m/%d")
        kmk_transacted['2nd_sales_consid']= pd.to_numeric(kmk_transacted['2nd_sales_consid'])
        kmk_transacted['hldg_len'] = kmk_transacted['2nd_sales_date']-kmk_transacted['instr_date']
        kmk_2 = kmk_transacted[kmk_transacted['hldg_len']>datetime.timedelta(days=365)] # filter out transactions within 1 year
        for ind in kmk_2['hldg_len'].index.to_list():
            kmk_2.loc[ind,'hldg_len'] = kmk_2.loc[ind, 'hldg_len'].days
        print(kmk_copy)
        kmk_2['inc_pyear']= (kmk_2['2nd_sales_consid']/kmk_2['consid']-1)/kmk_2['hldg_len']*365

        ##Set criteria for data selection for index construction
        #st_year =1997
        #next_year=2022
        st_date = datetime.datetime(st_year,1,1)
        year_change_max =0.2
        kmk_3 = kmk_2[kmk_2['instr_date']>=st_date] # transaction after st_date
        #kmk_3 = kmk_3[(kmk_3['inc_pyear']<year_change_max)&(kmk_2['inc_pyear']>-year_change_max)] # per year price change less than 20%
        #kmk_3 = kmk_3[ (kmk_3['nfa']> subj_nfa/1.5) & (kmk_3['nfa']< subj_nfa*1.5)] # size choice
        #kmk_3 = kmk_3[kmk_3['op_date']>st_date] #age selection as proxy for price range control

        ## Index Period set to by Month
        for year in range(st_year,next_year+1):
            for mon in range(1,13):
                year_mon = str(year)+"-"+str(mon)
                kmk_3[year_mon] = 0
        for index, row in kmk_3.iterrows():
            date = row.loc['instr_date']
            year = str(date.year)
            mon = str(date.month)
            tran_mon = year+'-'+mon
            kmk_3.at[index,tran_mon]=-1

        for index2, row2 in kmk_3.iterrows():
            date2 = row2.loc['2nd_sales_date']
            year2 = str(date2.year)
            mon2 = str(date2.month)
            tran_mon2 = year2+'-'+mon2
            kmk_3.at[index2,tran_mon2]=1
            #print(tran_mon)

        #print(kmk_3['2018-10'].to_list())

        ## Computing index
        kmk_3['log_p']= np.log((kmk_3['2nd_sales_consid']/kmk_3['consid']).astype('float64'))
        kmk_3=kmk_3.dropna(subset=['log_p'])
        kmk_3['p_change']= ((kmk_3['2nd_sales_consid']/kmk_3['consid']).astype('float64'))

        ## Regressing
        lm = LinearRegression(fit_intercept=False)
        data_end = len(kmk_3.columns)-2
        train_df= kmk_3.iloc[:,list(range(12,data_end+1))].dropna()
        X_train = train_df.iloc[:,:-1]
        y_train = train_df.iloc[:,-1]


        #display(kmk_3)
        #print(X_train)
        #print(y_train.to_list())


        #try:
        lm.fit(X_train,y_train)
        #except ValueError:
            #print('value error in:', district)
            #continue

        #print(lm.coef_*100)


        YY = (np.exp(lm.coef_)*100) # set 1997-1-1 level as 100
        y_axis = np.append([100],YY)
        x_axis = []
        for year in range(st_year,next_year+1):
            for mon in range(1,13):
                #for half in [1,16]:
                year_mon = str(year)+"-"+str(mon) #+ '-' + str(half)
                x_axis.append(year_mon)
        #x_axis=x_axis[7:]  #because 1998 - 2006 Jul data missing, so (12+7)
        kmk_price_index= pd.Series(y_axis, index = x_axis)
        kmk_price_index.to_csv(urlTo_valSystem+ '/val_sys/{}_price_index.csv'.format(area))


    # In[ ]:


    for district in district_df['d_code']:
        print(district)
        if district in ['HTT','HSSW','HSL','HRB','HP','HMW','HME','HJL','HCB','HC','HA']:
            print('Need replace')
            replace_url= urlTo_valSystem+ '/val_sys/HK_price_index.csv'
            kmk_price_index= pd.read_csv(replace_url, keep_default_na=False, index_col =0)
            kmk_price_index.to_csv(urlTo_valSystem+ '/val_sys/{}_price_index.csv'.format(district))
        if district in ['KSWK','KWTH']:
            print('Need replace')
            replace_url= urlTo_valSystem+ '/val_sys/KLN_price_index.csv'
            kmk_price_index= pd.read_csv(replace_url, keep_default_na=False, index_col =0)
            kmk_price_index.to_csv(urlTo_valSystem+ '/val_sys/{}_price_index.csv'.format(district))
        if district in ['NPSK']:
            print('Need replace')
            replace_url= urlTo_valSystem+ '/val_sys/NT_price_index.csv'
            kmk_price_index= pd.read_csv(replace_url, keep_default_na=False, index_col =0)
            kmk_price_index.to_csv(urlTo_valSystem+ '/val_sys/{}_price_index.csv'.format(district))



    # In[ ]:


    # Index Construction 3: Add index data for current and next month
    from dateutil.relativedelta import relativedelta

    for district in district_df['d_code']:
        #print(district)
        current_date= datetime.datetime.now().day
        current_month= datetime.datetime.now().month
        current_year= datetime.datetime.now().year
        cur_year_mon = str(current_year)+"-"+str(current_month)
        year_next_month= str(current_year)+"-"+str(current_month+1)
        previous_month= (datetime.datetime.now()+ relativedelta(months=-1)).month
        next_month= (datetime.datetime.now()+ relativedelta(months=1)).month
        next_year= (datetime.datetime.now()+ relativedelta(months=1)).year
        year_for_previous= (datetime.datetime.now()+ relativedelta(months=-1)).year
        previous_year_mon= str(year_for_previous)+"-"+str(previous_month)
        subj_price_index= pd.read_csv(urlTo_valSystem+ '/val_sys/{}_price_index.csv'.format(district), index_col=0, header=None,squeeze=True)

        if district[0]=="H":
            grouped_price_index= pd.read_csv(urlTo_valSystem+ '/val_sys/HK_price_index.csv'.format(district), index_col=0, header=None,squeeze=True)
        elif district[0]=="K":
            grouped_price_index= pd.read_csv(urlTo_valSystem+ '/val_sys/KLN_price_index.csv'.format(district), index_col=0, header=None,squeeze=True)
        else:
            grouped_price_index= pd.read_csv(urlTo_valSystem+ '/val_sys/NT_price_index.csv'.format(district), index_col=0, header=None,squeeze=True)


        if current_date<28:  # instr_date at oftens comes at least 3 weeks before, need to accumulate enough data
            subj_price_index[cur_year_mon]=subj_price_index[previous_year_mon]
        else:
            grouped_price_index= pd.read_csv(urlTo_valSystem+ '/val_sys/full_HK_price_index.csv', index_col=0, header=None,squeeze=True)
            subj_price_index[cur_year_mon]=(grouped_price_index[cur_year_mon]/grouped_price_index[previous_year_mon])*subj_price_index[previous_year_mon]
        """elif current_date>=10 and current_date<15:
            grouped_price_index= pd.read_csv(urlTo_valSystem+ '/val_sys/full_HK_price_index.csv', index_col=0, header=None,squeeze=True)
            subj_price_index[cur_year_mon]=(grouped_price_index[cur_year_mon]/grouped_price_index[previous_year_mon])*subj_price_index[previous_year_mon]
        elif current_date>=15 and current_date<20:
            subj_price_index[cur_year_mon]=(grouped_price_index[cur_year_mon]/grouped_price_index[previous_year_mon])*subj_price_index[previous_year_mon]"""

        subj_price_index[subj_price_index==100]=subj_price_index[cur_year_mon] # set all 100 value to cur year mon value, so 1997 one will be wrong
        subj_price_index['1997-1']=100 #set back 1997-1 value
        #subj_price_index[year_next_month]=subj_price_index[cur_year_mon]  #In case index is not updated in the first few day of a new month

        subj_price_index.to_csv(urlTo_valSystem+ '/val_sys/{}_price_index.csv'.format(district))
        print('finish price index')


    # # Use the Index to add time-adj npsf to data (Pre-requisite: time index built) and strim csv

    # In[ ]:


    for district in district_df['d_code'][:]:

        print(district)

        filtered_url =urlTo_valSystem+ '/raw_data/{}_filtered_wOtherArea.csv'.format(district)
        kmk_addTime =pd.read_csv(filtered_url, index_col=0 )
        price_url= urlTo_valSystem+ '/val_sys/{}_price_index.csv'.format(district)
        kmk_price_index=pd.read_csv(price_url, index_col=0 , header=None,squeeze=True)

        #print(kmk_addTime['consid'])
        kmk_addTime['npsf']= kmk_addTime['consid']*1000000/kmk_addTime['nfa']
        kmk_addTime['instr_date'] = pd.to_datetime(kmk_addTime['instr_date'], format="%Y-%m-%d")

        kmk_addTime= kmk_addTime[(kmk_addTime['instr_date']>=datetime.datetime(1995,1,1)) & (kmk_addTime['instr_date']<=datetime.datetime.now())]
        today_str= str(datetime.datetime.now().year)+'-'+str((datetime.datetime.now().month  )) #-datetime.timedelta(days=61)).month)  # Need amend
        print(today_str)
        #break
        for ind, row in kmk_addTime.iterrows():
            data_date=row.loc['instr_date']
            ## price_index goes only back to 1997 Jan
            if data_date.year<=1996:
                kmk_addTime.at[ind,'time_adj_npsf']=kmk_addTime.at[ind,'npsf']
                continue
            data_date_str =str(data_date.year)+'-'+str(data_date.month)
            #print(kmk_addTime.loc[ind,'npsf'],kmk_price_index[data_date_str])
            if kmk_price_index[data_date_str]==0 or kmk_price_index[data_date_str]==np.inf:
                kmk_price_index[data_date_str]=100
            #print(kmk_addTime.at[ind,'npsf'],kmk_price_index[data_date_str])
            kmk_addTime.at[ind,'time_adj_npsf']= kmk_addTime.at[ind,'npsf']/kmk_price_index[data_date_str]*kmk_price_index[today_str]
            ###Temporary Setting###
        #print(kmk_addTime['time_adj_npsf'])

        ## Strim csv
        strim_list=[ 'instr_date', 'final_address', 'time_adj_npsf',
           'gfa', 'nfa','other_area' , 'original_consid','consid', 'original_npsf','npsf',
           'memo_no', 'MW_data_code','nat_instr','d_code','code_chinese','op_date','op_year', 'bldg_age',
           'unit', 'floor', 'block', 'phase','est_name_en','est_name_ch','with_CPS'
           ]
        kmk_addTime=kmk_addTime[strim_list]

        kmk_addTime.to_csv(urlTo_valSystem+ '/raw_data/{}_filtered_IQR&specu_wTimenpsf.csv'.format(district))
        #Please manuelly copy and paste to val_sys or use below directly:
        kmk_addTime.to_csv(urlTo_valSystem+ '/val_sys/{}_filtered_IQR&specu_wTimenpsf.csv'.format(district))
    #pd.set_option('display.max_colwidth', -1)



    # # Add Coordinate to transaction

    # In[ ]:


    # bldg_address_wCoords_final.csv built from Find_Address_DEC.ipynb
    coords_df= pd.read_csv(urlTo_valSystem+"/val_sys/bldg_address_wCoords_final.csv",index_col =0)

    for district in district_df['d_code'][:]:
        trans_df= pd.read_csv(urlTo_valSystem+ '/val_sys/{}_filtered_IQR&specu_wTimenpsf.csv'.format(district),index_col=0)

        coords_sim= coords_df[['final_address','final_coords','lat','long']]
        df_wCoords=pd.merge(trans_df,coords_sim, on=['final_address'])
        df_wCoords.to_csv(urlTo_valSystem+ '/val_sys/{}_filtered_IQR&specu_wCoords.csv'.format(district)) #Not needed for upload
        print(district, 'completed')





    # # Adjustment Factor Development
