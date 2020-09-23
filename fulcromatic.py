from fulcrum import Fulcrum
import pyautogui
import rpa 
import os
import pandas as pd
import sqlalchemy

print('Welcome, my friend.  Libraries imported.')

fulcrum = Fulcrum(key=os.environ['FULCRUM_API'])
print('\n Fulcrum API connection established.')

db_host = "postgresqlsdsu.postgres.database.azure.com"
db_name = "fac_data_warehouse"
db_user = os.environ['AZURE_PG_UKEY']
db_password = os.environ['AZURE_PG_PW']
sslmode = "require"
#conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(db_host, db_user, db_name, db_password, sslmode)
#sql_engine = psycopg2.connect(conn_string) 
print("Azure Connection established"+'\n' + '=================================')  

#Production Engine
#engine = sqlalchemy.create_engine('postgresql+psycopg2://{}:{}@postgresqlsdsu.postgres.database.azure.com/fac_data_warehouse'.format(db_user, db_password))

#Local Test Engine
engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:Prost@780@localhost/fac_data_warehouse')

#Connect to Engine
conn = engine.connect() 




db_host = "postgresqlsdsu.postgres.database.azure.com"
db_name = "fac_data_warehouse"
db_user = os.environ['AZURE_PG_UKEY']
db_password = os.environ['AZURE_PG_PW']
sslmode = "require"
#conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(db_host, db_user, db_name, db_password, sslmode)
#sql_engine = psycopg2.connect(conn_string) 
print("Azure Connection established"+'\n' + '=================================')  

#Production Engine
#engine = sqlalchemy.create_engine('postgresql+psycopg2://{}:{}@postgresqlsdsu.postgres.database.azure.com/fac_data_warehouse'.format(db_user, db_password))

#Local Test Engine
engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:Prost@780@localhost/fac_data_warehouse')

#Connect to Engine
conn = engine.connect() 

sql='''
SELECT
room_key,
_title,
sfdb_number,
room_number,
signage_photos,
signage_photos_captions, 
door_hardware_photos,
door_hardware_photos_captions,
"360_photo",
"360_photo_captions",
room_feature_photos,
room_feature_photos_captions,
floor_cover_type,
restroom_accessible,
restroomambulatory,
restroom_accessible_transfer_side,
restroom_gender,
restroom_sink_count,
restroom_toilet_count,
restroom_urinal_count,
room_use_valid,
correct_room_use,
correct_station_count,
corrected_stations,
department_valid,
department_discrepancy_details,
correct_department,
floor_plan_valid,
floor_plan_correction_notes,
flag_as_mis_utilized,
survey_complete,
reason_not_surveyed,
not_surveyed_description,
survey_notes
FROM 
fulcrum.surveys_unprocessed
WHERE room_key IS NOT NULL;
'''.replace('\n', ' ')



df = pd.read_sql(sql, engine, index_col='_title')
df['room_key'] = df['room_key'].astype('int')

df = df.to_dict(orient='records')

os.chdir(r'C:\Data\fmatic')


for i in df:
    print(i)
    if i['signage_photos'] != None:
        if not os.path.exists(str(i['room_key'])):
            os.mkdir(str(i['room_key']))
            os.chdir(str(i['room_key']))
            print(os.getcwd())
            if not os.path.exists('signage_photos'):
                os.mkdir('signage_photos')
                os.chdir('./signage_photos')
                print('Signage photos directory created.')
            else:
                pass
        else:
            pass

        for rec in i['signage_photos'].split(','):
            
            file_name = i['sfdb_number'] + '-' + i['room_number'] + '-signage_' + rec[0:4]
            print(str(i['room_key']) + '-' + rec)
            photo = fulcrum.photos.media(rec)
            with open(f'{file_name}.jpg', 'wb') as f:
                f.write(photo)
            

        os.chdir(r'C:\Data\fmatic')
       
    