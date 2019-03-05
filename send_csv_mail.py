import urllib2
import settings
import os
import csv
import json
from datetime import date, timedelta
import datetime
import dateutil.parser
import sendgrid
import base64
from sendgrid.helpers.mail import *
import paramiko
import calendar

start_date = date.today() - timedelta(3)
yesterday = date.today() - timedelta(1)
today_date = date.today()
run_date_end = yesterday.strftime('20%y-%m-%d')
run_date_start = start_date.strftime('20%y-%m-%d')

def get_report_api(view):
    request = urllib2.Request(
        # uses a java script api service that returns a CSV

    return urllib2.urlopen(request).read()

def get_report_meta(report_name):
    i = 0
    json_data = open('./Reports/reports.json').read()
    data = json.loads(json_data)
    for reports in data:
        if reports["name"] == report_name:
            report = data[i]
        i += 1
    return report

def check_log(report):
    now = datetime.datetime.now()
    logs_data = open('./logs/schedule_log.json').read()
    log_data = json.loads(logs_data)
    if log_data==[]: return True
    else:
        for log in reversed(log_data): # if a report was sent in the last hour dont send again
            # print str(log) + '\n'
            delta_minutes = (now - dateutil.parser.parse(log["start_run"])).total_seconds() / 60
            if report["name"] == log["report"] and log["success"] == 1 and delta_minutes < 60:
                return False
            else:
                return True

def check_schedule(report):
    now = datetime.datetime.now()
    now_day=datetime.datetime(now.year,now.month,now.day)
    if report["schedule"]["interval"]=='Monthly':
        report_day = datetime.datetime.strptime(report["schedule"]["day_of_month"],'%Y-%m-%d')
    if report["schedule"]["interval"]=='Weekly':
        report_day = report["schedule"]["day_of_week"]
    report_time = datetime.datetime(now.year,now.month,now.day,report["schedule"]["hour"],report["schedule"]["minute"],00)
    delta_minutes = (now-report_time).total_seconds()/60
    if report["schedule"]["interval"]=='Daily' and abs(delta_minutes)<15 and check_log(report):
        json_log = {"report": report["name"], "state": report["state"], "Interval": report["schedule"]["interval"],
                    "schdeuled_run": report_time.isoformat(), "start_run": now.isoformat(), "mail": 0, "ftp": 0,
                    "success": 0}
        record = log_schedule(json_log)
        # print str(now - timedelta(minutes=30)) + " < " + report["name"] + " < " +str(now + timedelta(minutes=30))
        return [True,record]
    elif report["schedule"]["interval"]=='Monthly' and now_day.day==report_day.day and abs(delta_minutes)<15 and check_log(report):
        json_log = {"report": report["name"], "state": report["state"], "Interval": report["schedule"]["interval"],
                    "schdeuled_run": report_time.isoformat(), "start_run": now.isoformat(), "mail": 0, "ftp": 0,
                    "success": 0}
        record = log_schedule(json_log)
        return [True, record]
    elif report["schedule"]["interval"]=='Weekly' and calendar.day_name[now_day.weekday()]==report_day and abs(delta_minutes)<15 and check_log(report):
        json_log = {"report": report["name"], "state": report["state"], "Interval": report["schedule"]["interval"],
                    "schdeuled_run": report_time.isoformat(), "start_run": now.isoformat(), "mail": 0, "ftp": 0,
                    "success": 0}
        record = log_schedule(json_log)
        return [True, record]
    else:
        return [False,-1]

def log_schedule(json_log):
    DATA_FILENAME = settings.LOG_PATH+'schedule_log.json'
    json_data = open(DATA_FILENAME).read()
    data = json.loads(json_data)
    data.append(json_log)
    with open(DATA_FILENAME, 'w') as file:
        json.dump(data, file)
    return len(data)-1

def update_run_result(record,result,delivery):
    DATA_FILENAME = settings.LOG_PATH+'schedule_log.json'
    json_data = open(DATA_FILENAME).read()
    data = json.loads(json_data)
    data[record][delivery]=result
    with open(DATA_FILENAME, 'w') as file:
        json.dump(data, file,indent=4)
    return True

def get_report_csv(contents, report):
    filename = './Temp/' + report["output_file"] + '_' + today_date.strftime('20%y-%m-%d') + '.csv'
    json_parsed = json.loads(contents)
    if report["fields"] == []:
        field_list = json_parsed["meta"]["fields"]
    else:
        field_list = report["fields"]
    f = csv.writer(open(filename, "wb+"))
    # building the report structure automatically
    # if order matters specify a different list in reports.json file
    # print field_list
    # Write CSV Header, If you dont need that, remove this line
    f.writerow(field_list)
    structure = '['
    for i, field in enumerate(field_list):
        if i:  # print a separator if this isn't the first element
            structure = structure + ',\n'
        structure = structure + 'mydict["' + field + '"]'
    structure = structure + ']'
    for json_parsed["data"] in json_parsed["data"]:
        mydict = {k: unicode(v).encode("utf-8") for k, v in json_parsed["data"].iteritems()}        # print mydict
        f.writerow(eval(str(structure)))

def send_file(report):
    filename = report["output_file"] + '_' + today_date.strftime('20%y-%m-%d') + '.csv'
    filepath = './Temp/' + filename
    destination = report["ftp"]["path"] + filename
    transport = paramiko.Transport((report["ftp"]["url"], 22))
    transport.connect(username=report["ftp"]["user"], password=report["ftp"]["password"])
    sftp = paramiko.SFTPClient.from_transport(transport)
    sftp.put(filepath, destination)

def send_mail(report,error):
    if error != '' :
        report["mail"]["recipients"].append('application-support@hello.com')
        report["mail"]["subject"]=report["mail"]["subject"] + ' - ERROR ' + error
    print 'Sending... ' + report["mail"]["subject"]
    for email in report["mail"]["recipients"]:
        filename = report["output_file"] + '_' + today_date.strftime('20%y-%m-%d') + '.csv'
        sg = sendgrid.SendGridAPIClient(apikey=settings.SENDGRID_API_KEY)
        from_email = Email("Data&Analytics@hello.com")
        to_email = Email(email)
        subject = report["mail"]["subject"] + ' - Tableau Report Delivery'
        content = Content("text/plain", "Email Attachments from Tableau Reports")

        # file_path = "file_path.pdf"
        with open('./Temp/' + filename, 'rb') as f:
            data = f.read()
            f.close()
        encoded = base64.b64encode(data).decode()

        attachment = Attachment()
        attachment.content = encoded
        attachment.type = "application/pdf"
        attachment.filename = filename
        attachment.disposition = "attachment"
        attachment.content_id = "Example Content ID"

        mail = Mail(from_email, subject, to_email, content)
        mail.add_attachment(attachment)
        response = sg.client.mail.send.post(request_body=mail.get())

        print(response.status_code)
        print(response.body)
        print(response.headers)

def main():
    json_data = open('./Reports/reports.json').read()
    data = json.loads(json_data)
    reports_ran = 0
    # looping to run a scheduler
    for report in data:
        if report["state"] == "active" :    #going through all active saved reports
            error_count=0
            check_result = check_schedule(report) #check for the ones that have a current schdeule [true,record number]
            if check_result[0]:
                reports_ran +=1
                print 'Running... ' + report["name"]
                file_path = './Temp/' + report["output_file"] + '_' + today_date.strftime('20%y-%m-%d') + '.csv'
                content = get_report_api(report["source_view"])
                get_report_csv(content, report)
                if report["mail"]["active"] == "yes":
                    send_mail(report,'')
                    update_run_result(check_result[1], 1,'mail')  # transfer current record number, result status
                if report["ftp"]["active"] == "yes":
                    try:
                        send_file(report)
                        update_run_result(check_result[1], 1,'ftp')
                    except:
                        error_count+=1
                        send_mail(report,'FTP')
                        update_run_result(check_result[1], -1, 'ftp')
                os.remove(file_path)
                if error_count>=1: update_run_result(check_result[1], -1, 'success')
                else: update_run_result(check_result[1], 1, 'success')

    print str(reports_ran) + ' reports ran in the last hour'

if __name__ == '__main__':
    main()

