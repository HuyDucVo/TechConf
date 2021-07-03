import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
#from sendgrid import SendGridAPIClient
#from sendgrid.helpers.mail import Mail

def main(msg: func.ServiceBusMessage):
    try:
        notification_id = int(msg.get_body().decode('utf-8'))
        
        # TODO: Get connection to database
        m_dbname = "techconfdb"
        m_user = "techconfdb@techconfdb"
        m_password = "Megaaxl95."
        m_host = "techconfdb.postgres.database.azure.com"
        psycopg2.conn = psycopg2.connect(
            dbname= m_dbname, 
            user= m_user,
            password= m_password,
            host= m_host)

        m_cursor = psycopg2.conn.cursor()

    
        # TODO: Get notification message and subject from database using the notification_id
        m_cursor.execute("SELECT message, subject FROM notification where id = {};".format(notification_id))
        m_message_and_subject = m_cursor.fetchone()
        logging.info("----- MESSAGE/SUBJECT ----- {}".format(m_message_and_subject))

        # TODO: Get attendees email and name
        m_cursor.execute("SELECT email, first_name, last_name FROM attendee;")
        m_attendees = m_cursor.fetchall()
        logging.info("----- ATTENDEES ----- {}".format(m_attendees))

        # TODO: Loop through each attendee and send an email with a personalized subject
        for attendee in m_attendees:
            subject = "{}: {}".format(attendee[1], m_message_and_subject[1])
            #email()
            logging.info("----- EMAIL ----- {} ---- {} ---- {}".format(attendee[0], subject, m_message_and_subject[0]))

        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        m_status = "Notified: {}".format(len(m_attendees))
        logging.info("----- NOTIFICATION SUBMITTED ----- ")
        logging.info("----- NOTIFIED ----- {} attendees".format(len(m_attendees)))
        m_cursor.execute("Update notification set status = {}, completed_date = {} where id = {} ".format(m_status, datetime.now(), notification_id))
        query = "Update notification set status = %s, completed_date = %s where id = %s"
        m_cursor.execute(query,(m_status, datetime.utcnow(), notification_id))
        psycopg2.conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally :
        # TODO: Close connection
        logging.info("Python ServiceBus queue trigger finished")
        # TODO: Close connection
        m_cursor.close()
        psycopg2.conn.close()