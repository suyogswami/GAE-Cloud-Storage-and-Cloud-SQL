#################################################################################
##  Name: Suyog S Swami
##  ID: 1001119101
##  Course: CSE6331-Cloud Computing -- Batch: 1PM to 3PM
##  Programming Assignment 2: Platform as a service(Google cloud-- Database)
##
##  Reference List:
##                  1. https://cloud.google.com/appengine/docs/whatisgoogleappengine
##                  2. https://cloud.google.com/appengine/docs/python/cloud-sql/
##                  3. https://cloud.google.com/appengine/docs/python/gettingstartedpython27/helloworld
##                  4. https://cloud.google.com/appengine/docs/python/gettingstartedpython27/templates
##                  5. https://cloud.google.com/appengine/docs/python/blobstore/
#################################################################################

import time
import webapp2
from google.appengine.ext.webapp.util import run_wsgi_app
import csv
import MySQLdb
import os
from google.appengine.ext import blobstore
from google.appengine.ext.webapp import blobstore_handlers

# Jinja2 environment
import jinja2
JINJA_ENVIRONMENT = jinja2.Environment(
  loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
  autoescape=True,
  extensions=['jinja2.ext.autoescape'])

_INSTANCE_NAME = 'earthquake-980:shake' # Project ID : Instance name of the project created

class UploadFormHandler(webapp2.RequestHandler):
    def get(self):
        upload_url = blobstore.create_upload_url('/upload_photo',gs_bucket_name='shaking-bucket') # Bucket_Name from the created project
        self.response.out.write('<html><body>')
        self.response.out.write('<form action="%s" method="POST" enctype="multipart/form-data">' % upload_url)
        self.response.out.write('''Upload File: <input type="file" name="file"> <br> <input type="submit" name="submit" value="Submit"> </form></body></html>''')

class UploadHandler(blobstore_handlers.BlobstoreUploadHandler):
    def post(self):
        if (os.getenv('SERVER_SOFTWARE') and
            os.getenv('SERVER_SOFTWARE').startswith('Google App Engine/')):
            db = MySQLdb.connect(unix_socket='/cloudsql/' + _INSTANCE_NAME, port=3306, db='moving', user='root',passwd='') #Used when deployed
        else:
            db = MySQLdb.connect(host='localhost', port=3306, db='moving', user='suyog', passwd='suyog') #used by launcher
        t1=time.time()
        upload = self.get_uploads()[0] # Upload csv to the bucket
        t2=time.time()
        t3=t2-t1
        self.response.out.write('<html><body><center>File Uploaded Successfully !!</center>' )
        self.response.out.write('<br>' )
        self.response.out.write('<center>Time required to upload the file to cloud bucket is : &nbsp' )
        self.response.write(t3)
        self.response.out.write('</center><br><br>')
        #cursor1 = db.cursor()
        #cursor1.execute('create table earthquaked(time DATETIME(6),latitude float,longitude float,depth float,mag float,magType VARCHAR(255),nst float,gap float,dmin float,rms VARCHAR(255),net float,id VARCHAR(255),updated DATETIME(6),place VARCHAR(255),type VARCHAR(255))')

        blob_key=upload.key()   # Get the key of the data uploaded in the bucket.
        self.response.out.write('<center>Reading Records from bucket !!</center>' )
        self.response.out.write('<br>')
        t4=time.time()
        blob_reader=blobstore.BlobReader(blob_key) # read the data in bucket that is currently inserted
        reader=csv.reader(blob_reader)
        next(reader)
        cursor = db.cursor()
        self.response.out.write('<center>Inserting Records into Database !!</center>' )
        self.response.out.write('<br>')
        for r in reader:
            tym=str(r[0])
            latitude=str(r[1])
            longitude=str(r[2])
            depth=str(r[3])
            mag=str(r[4])
            mag_type=str(r[5])
            nst=str(r[6])
            gap=str(r[7])
            dmin=str(r[8])
            rms=str(r[9])
            net=str(r[10])
            i_d=str(r[11])
            updated=str(r[12])
            place=str(r[13])
            typ=str(r[14])
            # Note that the only format string supported is %s # Insert into the instance database.
            cursor.execute('INSERT INTO earthquaked VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s)', (tym,latitude,longitude,depth,mag,mag_type,nst,gap,dmin,rms,net,i_d,updated,place,typ))
            #db.close()
        t5=time.time()
        t6=t5-t4
        self.response.out.write('<center>Time for Uploading to Cloud SQL Database=&nbsp' )
        self.response.out.write(t6)
        self.response.out.write('<br></center></body></html>' )


        # determine the number of earthquakes for each week, for magnitudes 2, 3, 4, and 5 (or greater)
        for wk in range(18,24):
            cursor.execute('select extract(week from time),cast(mag as unsigned) as magnitude, count(*) from earthquaked where extract(week from time) =%s and mag between 0 and 10 group by cast(mag as unsigned)',(wk))
            guestlist = [];
            for r in cursor.fetchall():
                #Data displayed on main.html using Jinja2.

                guestlist.append(dict([('Week',r[0]),
                                 ('Magnitude',r[1]),
                                 ('Count',r[2])
                                 ]))
            variables = {'guestlist': guestlist}
            template = JINJA_ENVIRONMENT.get_template('main.html')
            self.response.write(template.render(variables))


        db.commit()
        db.close()

app = webapp2.WSGIApplication([('/', UploadFormHandler),
                               ('/upload_photo', UploadHandler),
                              ], debug=True)

run_wsgi_app(app)