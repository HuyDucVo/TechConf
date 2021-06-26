import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def main(msg: func.ServiceBusMessage):

    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)
    
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

    try:
        # TODO: Get notification message and subject from database using the notification_id
        m_cursor.execute("SELECT message, subject FROM notification where id = {};".format(notification_id))
        m_message_and_subject = m_cursor.fetchone()
        logging.info("----- MESSAGE/SUBJECT ----- {}",m_message_and_subject)

        # TODO: Get attendees email and name
        m_cursor.execute("SELECT email, first_name, last_name FROM attendee;")
        m_attendees = m_cursor.fetchall()
        logging.info("----- ATTENDEES ----- {}".format(m_attendees))

        # TODO: Loop through each attendee and send an email with a personalized subject
        for attendee in m_attendees:
            subject = "{}: {}".format(attendee[1], m_message_and_subject[1])
            # send_email(attendee[0], subject, notification[0])
            logging.info("----- EMAIL ----- {} ---- {} ---- {}".format(attendee[0], subject, m_message_and_subject[0]))

        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        m_status = "Notified: {}".format(len(m_attendees))
        logging.info("----- NOTIFIED -----".format(len(m_attendees)))
        m_cursor.execute("Update notification set status = %s, completed_date = %s where id = %s".format(m_status, datetime.now(), notification_id))
        psycopg2.conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        # TODO: Close connection
        logging.info("Python ServiceBus queue trigger finished")
        # TODO: Close connection
        m_cursor.close()
        psycopg2.conn.close()

def send_email(email, subject, body):
    message = Mail(
        from_email='info@techconf.com', 
        to_emails=email, subject=subject, 
        plain_text_content=body)    
    SENDGRID_API_KEY = "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"
    sg = SendGridAPIClient(SENDGRID_API_KEY)
    sg.send(message)