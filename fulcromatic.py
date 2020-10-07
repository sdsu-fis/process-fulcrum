from fulcrum import Fulcrum
import pyautogui
import rpa 
import os
import pandas as pd
import sqlalchemy

print('Welcome, my friend.  Libraries imported.')

fulcrum = Fulcrum(key=os.environ['FULCRUM_API'])
print('\n Fulcrum API connection established.')


export_dir = r'C:\Data\fmatic'

os.chdir(export_dir)


db_host = "postgresqlsdsu.postgres.database.azure.com"
db_name = "fac_data_warehouse"
db_user = os.environ['AZURE_PG_UKEY']
db_password = os.environ['AZURE_PG_PW']
sslmode = "require"



#Production Engine
engine = sqlalchemy.create_engine('postgresql+psycopg2://{}:{}@postgresqlsdsu.postgres.database.azure.com/fac_data_warehouse'.format(db_user, db_password))
print("Azure Connection established"+'\n' + '=================================')  



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



print('Fetching unprocesed surveys from FDW\n')
df = pd.read_sql(sql, engine)
df['room_key'] = df['room_key'].astype('int')
df.to_csv('records.csv')

print('Converting to dictionary\n')
df = df.to_dict(orient='records')



photos_to_process = ['signage_photos', 'door_hardware_photos','room_feature_photos']



print('\n---------- Processing signage photos ----------\n')



for i in df:
    
    for li in photos_to_process:
        if i[li] != None:
            print('\n' + i['_title'] + ' contains ' + str(len(i[li].split(','))) + f'{li}')

            #Check to see if there's already a dir for the given room.  If not, make it.  
            if not os.path.exists(str(i['room_key'])):
                os.mkdir(str(i['room_key']))
                #os.chdir(str(i['room_key']))
                print('Created directory:' + os.getcwd())

            

            if not os.path.exists(li):
                
                os.chdir(str(i['room_key']))
                os.mkdir(li)
                os.chdir(f'./{li}')
                print(os.getcwd())
                print(f'{li} directory created.') 
                    
            else:
            
                os.chdir(str(i['room_key']))

            for rec in i[li].split(','):
                
                #Get the index position of the photo
                index_pos = i[li].split(',').index(rec)
                print('Image index position: ' + str(index_pos))

                #Add a suffix as a means of dealing with multiple photos so they don't have duplicate names
                suffix = index_pos + 1
                print('Suffix: ' + str(suffix))

                if i[f'{li}_captions'] == None or i[f'{li}_captions'].split(',')[index_pos] == '':
                    print('No captions.  Using default.')
                    file_name = i['sfdb_number'] + '-' + i['room_number'] + f"-{li.replace('_photos', '')}-" + str(suffix)

                elif i[f'{li}_captions'] != None:
                    print('Images contain captions.')
                    print(i[f'{li}_captions'].split(',')[index_pos])
                    file_name =  i['sfdb_number'] + '-' + i['room_number']  + '-' + str(i[f'{li}_captions'].split(',')[index_pos])
                
                else:
                    file_name = i['sfdb_number'] + '-' + i['room_number'] + f"-{li.replace('_photos', '')}-" + str(suffix)
                    
                
                
                
                #Get the photo object from Fulcrum API
                photo = fulcrum.photos.media(rec)
                with open(f'{file_name}.jpg', 'wb') as f:
                    f.write(photo)
                print(f'{file_name}.jpg' + ' saved')
            
            print(f'---------- Finished with {li}! ----------\n')
            os.chdir(export_dir)

    #Get 360 photo
    print('\nGetting 360 photo')

    if i['360_photo'] != None:
        rkey = str(i['room_key'])
        
        if not os.path.exists(str(i['room_key'])):
            os.mkdir(str(i['room_key']))
            os.chdir(str(i['room_key']))
            print('Created directory:' + os.getcwd())
        
        os.chdir(os.path.join(export_dir, str(rkey)))
        print(os.getcwd())
        os.mkdir('./360_photo')
        
        os.chdir('./360_photo')
        print(os.getcwd())
        

        file_name = i['sfdb_number'] + '-' + i['room_number'] + '-360'

        photo = fulcrum.photos.media(i['360_photo'])
        with open(f'{file_name}.jpg', 'wb') as f:
            f.write(photo)
        print(f'{file_name}.jpg' + ' saved')
        print('---------- Finished with 360 photo! ----------\n')


    os.chdir(export_dir)

print('Room and photo downloads complete.\nGetting serialized equipment records and photos.')
    import json
'''
Get parent id room key mappings.  
This is needed because the child records aka repeatable sections
don't have any of the parent record information except the record id
'''

equipment_base_dir = r'C:\Data\fmatic\serialized_equipment'
sql = "Select _record_id,room_key from \"Space Inventory Field Survey Version 0.3\" WHERE survey_processed = 'no'"
resp = fulcrum.query(sql, 'geojson')

#Temp holders for the values
parent_id = []
record_rkey = []

#
for i in resp['features']:
    parent_id.append(i['properties']['_record_id'])
    record_rkey.append(i['properties']['room_key'])

    
#This contains parent record id room key pairs.  Used later to get rkey values    
parent_map = dict(zip(parent_id,record_rkey))


sql = "SELECT e.* from \"Space Inventory Field Survey Version 0.3/serialized_equipment\" e WHERE equipment_record_processed IS NULL OR equipment_record_processed = 'no'"

resp = fulcrum.query(sql, 'geojson')

os.chdir(r'C:\Data\fmatic')
if not os.path.exists('.\serialized_equipment'):
    os.mkdir('serialized_equipment')

with open('serialized_equipment.json', 'w') as outfile:
    json.dump(resp, outfile)


parent_ids = []
parent_ids = [record['properties']['_parent_id'] for record in resp['features'] ]

os.chdir('.\serialized_equipment')

for rec in resp['features']:
    
    assetid = rec['properties']['asset_id']
    
    
    if rec['properties']['_parent_id'] in parent_map.keys():
        rkey = parent_map.get(rec['properties']['_parent_id'])
    else:
        rkey = '000'
    
    dirname = str(rkey + '-' + assetid).replace(' ', '_')
    if not os.path.exists(dirname):
        os.mkdir(dirname)
        print('Creating directory: ' + dirname)
    else:
        pass
    print(dirname + ' already exists.')
    
    os.chdir(dirname)
    
   
    #Get photos
    if rec['properties']['equipment_photos'] == None:
        pass
    else:
        i = 0
        suffix = 1
        
        
  
        for photo in rec['properties']['equipment_photos']:
            if rec['properties']['equipment_photos_captions'][i] == None:
                filename = rec['properties']['asset_id']+ '-' + str(suffix) + '.jpg'
            else:
                filename = str(rec['properties']['equipment_photos_captions'][i]) + '.jpg'
            
            img = fulcrum.photos.media(photo)

            with open(filename, 'wb') as f:
                f.write(img)
            print(filename + ' saved')
            i +=1
            suffix += 1
    os.chdir(equipment_base_dir)
    


       
    