import io
from io import StringIO
import boto3
from botocore.exceptions import NoCredentialsError
import os
import pandas as pd
from scipy.signal import argrelextrema, find_peaks
import pyodbc
import datetime as dt
from datetime import datetime
from datetime import date
import pymysql
from sqlalchemy import create_engine

print(str(dt.datetime.now() ))

os.environ["AWS_ACCESS_KEY_ID"] = "AKIAI43W5A2N6ICINNHA"
os.environ["AWS_SECRET_ACCESS_KEY"] = "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"

filename=str('exercisedata2020-12-12 12:12:56.32518100:00:00.csv')

s3_client = boto3.client('s3')
response = s3_client.get_object(Bucket="ripclipbucket",Key=filename)
#Need to make the file name dynamic
file = response["Body"].read()


data=pd.read_csv(io.BytesIO(file), header=0, delimiter=",", low_memory=False)

# data=pd.DataFrame(data,columns= ['Time','Timer','Exercise_Id','date','Time','Timer','Load','XAcceleration','YAcceleration','ZAcceleration','X_Rotation','Y_Rotation','Z_Rotation','X_Velocity',
# 'Y_Velocity','Z_Velocity','X_Distance','Y_Distance','Z_Distance','Rep','Set'])

for index, row in data.iterrows():
    if index ==0:
        data.loc[index,'Zvelo'] =0
    else:
        data.loc[index,'Zvelo'] =(row['ZAccel']-data['ZAccel'][index-1])/(row['Timer']-data['Timer'][index-1])+data['Zvelo'][index-1]
    if index ==0:
        data.loc[index,'Xvelo'] =0
    else:
        data.loc[index,'Xvelo'] =(row['XAccel']-data['XAccel'][index-1])/(row['Timer']-data['Timer'][index-1])+data['Xvelo'][index-1]
    if index ==0:
        data.loc[index,'Yvelo'] =0
    else:
        data.loc[index,'Yvelo'] =(row['YAccel']-data['YAccel'][index-1])/(row['Timer']-data['Timer'][index-1])+data['Yvelo'][index-1]
        #print(data)




data['Xvelo']=data['Xvelo'].fillna(0)
data['Yvelo']=data['Yvelo'].fillna(0)
data['Zvelo']=data['Zvelo'].fillna(0)



for index, row in data.iterrows():
    if index ==0:
        data.loc[index,'Z_Distance'] =0
    else:
        data.loc[index,'Z_Distance'] =(row['Zvelo']-data['Zvelo'][index-1])/(row['Timer']-data['Timer'][index-1])+data['Z_Distance'][index-1]
    if index ==0:
        data.loc[index,'X_Distance'] =0
    else:
        data.loc[index,'X_Distance'] =(row['Xvelo']-data['Xvelo'][index-1])/(row['Timer']-data['Timer'][index-1])+data['X_Distance'][index-1]
    if index ==0:
        data.loc[index,'Y_Distance'] =0
    else:
        data.loc[index,'Y_Distance'] =(row['Yvelo']-data['Yvelo'][index-1])/(row['Timer']-data['Timer'][index-1])+data['Y_Distance'][index-1]
        #print(data)




data['Xvelo']=data['Xvelo'].fillna(0)
data['Yvelo']=data['Yvelo'].fillna(0)
data['Zvelo']=data['Zvelo'].fillna(0)


for index, row in data.iterrows():
    if index ==0:
        data.loc[index,'Peak'] =0
        data.loc[index,'Reps'] =0
    else:
        peaks, _ = find_peaks(data[data["Zvelo"].notna()]['Zvelo'], distance=60, prominence=150) 
        #current distnace and prominace is based on this data set.
        #When we get real data they will probably have to change and will likely need to be set to update to user and exercise
        #Goal would be to have a default Dist and Prominamce for each exercise that then updates by user
        #Something like getting the mean and standard deviation of dist and pro and 1 stdev below to track 

        #print(peaks)
        
        for a in range(len(peaks)):
            if index>=peaks[a]:
                data.loc[index,'Reps'] = a+1
                a=a+1


data['Reps']=data['Reps'].fillna(0)



bucket = 'ripclipbucket' # already created on S3
csv_buffer = StringIO()
data.to_csv(csv_buffer)
s3_resource = boto3.resource('s3')
s3_resource.Object(bucket, filename).put(Body=csv_buffer.getvalue())




conn = pyodbc.connect('Driver={SQL Server};Server=ripclip3.czi81renb7nj.us-east-2.rds.amazonaws.com;Database=Ripclip;uid=admin;pwd=RipClip1')
cursor = conn.cursor()


max_value_date = str(date.today())
max_value_user = data["User_Id"].max()
max_value_exercise = data["Exercise_Id"].max()
cursor.execute("select isnull(max([Set]),0) from Raw_Data where Date=? and User_Id=? and Exercise_Id =?",max_value_date,max_value_user,max_value_exercise)
maxset=cursor.fetchall()
maxset=maxset[0][0]+1



cursor.fast_executemany = True
for row in data.itertuples():
    
    cursor.execute('''
                INSERT INTO Ripclip.dbo.Raw_Data ([User_Id],[Bar_Id],[Exercise_Id],[date],[Time],[Timer],
                [Load],[X_Acceleration],[Y_Acceleration],[Z_Acceleration],[X_Rotation],[Y_Rotation],[Z_Rotation],
                [X_Velocity],[Y_Velocity],[Z_Velocity],[Rep],[Set],[X_Distance],[Y_Distance],[Z_Distance])
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ''',
                row.User_Id,
                row.Bar_Id,
                row.Exercise_Id,
                str(date.today()),
                row.Time,
                row.Timer,
                row.Load,
                row.XAccel, 
                row.YAccel,
                row.ZAccel,
                row.XRot,
                row.YRot,
                row.ZRot,
                row.Xvelo,
                row.Yvelo,
                row.Zvelo,
                row.Reps,
                maxset,
                row.X_Distance,
                row.Y_Distance,
                row.Z_Distance

              

                )
conn.commit()
print(str(dt.datetime.now() ))
