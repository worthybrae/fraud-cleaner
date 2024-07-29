import boto3
import datetime
import math
import os
import pandas as pd
import matplotlib.pyplot as plt
from csv import writer
from decouple import config
from email.utils import make_msgid
import smtplib
import mimetypes
from email.message import EmailMessage
from email.mime.application import MIMEApplication
from botocore.exceptions import BotoCoreError, ClientError


bucket_dict = {
    'in-arctictern': {
        'vendor_id': 640,
        'prefix': 'processed/daily/'
    },
    'in-minke': {
        'vendor_id': 655,
        'prefix': 'processed/live/'
    },
    'in-manta': {
        'vendor_id': 540,
        'prefix': 'processed/live-feed/'
    },
    'in-matamata': {
        'vendor_id': 740,
        'prefix': 'processed/live/'
    },
    'in-clownfish': {
        'vendor_id': 533,
        'prefix': 'processed/'
    },
    'in-cuttlefish': {
        'vendor_id': 736,
        'prefix': 'processed/live/'
    },
    'in-triggerfish': {
        'vendor_id': 551,
        'prefix': 'processed/'
    },
    'in-greenland': {
        'vendor_id': 554,
        'prefix': 'processed/'
    },
    'in-barracuda': {
        'vendor_id': 742,
        'prefix': 'hem/'
    }
}

vendor_list = [640, 655, 540, 740, 533, 736, 551, 554, 742]


def create_s3_object():
    session = boto3.Session(
        aws_access_key_id=config('AWS_KEY_ID'),
        aws_secret_access_key=config('AWS_SECRET_KEY'),
        region_name='us-east-1'
    )
    s3 = session.resource('s3')
    return s3


def get_bucket_stats(bucket_name, bucket_prefix):
    s3 = boto3.client('s3')
    
    file_count = 0
    file_size_cumulative = 0
    
    try:
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=bucket_prefix)
        
        for page in page_iterator:
            if 'Contents' in page:
                for file in page['Contents']:
                    file_count += 1
                    file_size_cumulative += file['Size']
                    
        return file_count, file_size_cumulative
    except (BotoCoreError, ClientError, TypeError) as e:
        print(f"An error occurred: {e}")
        return 0, 0


def generate_date_strings(lookback_period):
    date_strings = []
    for i in range(1, lookback_period+1):
        date_string = (datetime.date.today() - datetime.timedelta(days=i)).strftime('%Y/%m/%d')
        date_strings.append(date_string)
    return date_strings


def convert_size(num, pos):
    if num == 0:
        return ""
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(num, 1024)))
    p = math.pow(1024, i)
    s = round(num / p, 1)
    return "%s %s" % (s, size_name[i])


def read_data(file_name):
    df = pd.read_csv(file_name)
    dfp = df.pivot_table(index=df['Date'], columns='Vendor ID', values='Total Size', aggfunc='sum')
    return dfp


def generate_vendor_messages(dfp):
    alerts = ''
    warnings = ''
    for vendor in vendor_list:
        recent_val = dfp[vendor].iloc[-1]
        vendor_mean = dfp[vendor].iloc[:-1].mean()
        vendor_std = dfp[vendor].iloc[:-1].std()
        if recent_val < vendor_mean - vendor_std * 3:
            format_size = convert_size(recent_val, 0)
            if format_size == '':
                alerts += f'<li>Vendor {vendor} delivered no data today which is unusually low</li>'
            else:
                alerts += f'<li>Vendor {vendor} delivered {format_size} of data today which is unusually low</li>'
        elif recent_val == 0:
            warnings += f'<li>Vendor {vendor} delivered no data today but has high inflow variability</li>'
    return warnings, alerts


def generate_area_graph(dfp, file_name):
    graph_name = file_name.split('.')[0] + '.png'
    ax = dfp.plot.area(stacked=True)
    ax.yaxis.set_major_formatter(convert_size)
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles[::-1], labels[::-1], bbox_to_anchor=(1.05, 1.0), loc='upper left')
    ax.margins(x=0)
    plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    plt.title('Data Inflow By Vendor Over The Past 7 Days')
    plt.tight_layout()
    plt.rcParams["font.family"] = "Arial"
    plt.savefig(graph_name, dpi=300)
    return graph_name


def send_mail(send_from, send_to, subject, warnings, alerts, graph, files=None):
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(config('GMAIL_EMAIL'), config('GMAIL_APP_PASSWORD'))

    msg = EmailMessage()
    msg['From'] = send_from
    msg['To'] = send_to
    msg['Subject'] = subject

    msg.set_content('View data inflow performance by vendor and check data inflow alerts...')

    image_cid = make_msgid(domain='a6.com')

    msg.add_alternative("""\
    <html>
        <body style="width: 100%; height: 100%; margin: 0; padding: 0;">
            <div style="width: 100%; height: 100%; display: flex;">
                <div style="width: 30%; background-color: #ececec; border-radius: 15px; padding: 5px 25px;">
                    <div>
                        <h1 style="font-style: bold; font-family: ‘Open Sans’, Arial, sans-serif; font-size: 24px;">⚠️  Warnings</h1>
                        <ul style="font-style: normal; font-family: ‘Open Sans’, Arial, sans-serif; font-size: 16px; padding: 0px 15px;">
                            {warnings}
                        </ul>
                    </div>
                    <hr style="color: white;" />
                    <div>
                        <h1 style="font-style: bold; font-family: ‘Open Sans’, Arial, sans-serif; font-size: 24px;">🚨  Alerts</h1>
                        <ul style="font-style: normal; font-family: ‘Open Sans’, Arial, sans-serif; font-size: 16px; padding: 0px 15px;">
                            {alerts}
                        </ul>
                    </div>
                </div>
                <div style="width: 5%"></div>
                <div style="display: flex; align-items: center; justify-content: center; width: 65%;">
                    <img src="cid:{image_cid}" style="width: 100%;">
                </div>
            </div>
        </body>
    </html>
    """.format(image_cid=image_cid[1:-1], warnings=warnings, alerts=alerts), subtype='html')

    with open(graph, 'rb') as img:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img.name)[0].split('/')

        # attach it
        msg.get_payload()[1].add_related(img.read(),
                                         maintype=maintype,
                                         subtype=subtype,
                                         cid=image_cid)

    for f in files or []:
        with open(f, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=f
            )
        # After the file is closed
        part['Content-Disposition'] = 'inline; filename="%s"' % f
        msg.attach(part)

    server.sendmail(send_from, send_to, msg.as_string())


if __name__ == '__main__':
    s3 = create_s3_object()
    lookback_period = 30
    date_strings = generate_date_strings(lookback_period)
    start = datetime.datetime.strptime(date_strings[0], '%Y/%m/%d').strftime('%Y-%m-%d')
    end = datetime.datetime.strptime(date_strings[lookback_period-1], '%Y/%m/%d').strftime('%Y-%m-%d')
    with open(f'data-inflows-{end}-to-{start}.csv', 'a') as f_object:
        writer_object = writer(f_object)
        writer_object.writerow(['Date', 'Vendor ID', 'Total Files', 'Total Size'])
        for date_string in date_strings:
            print(f'Data Inflow for {date_string} processing...')
            for bucket_name in bucket_dict:
                bucket = s3.Bucket(bucket_name)
                bucket_prefix = bucket_dict[bucket_name]['prefix'] + date_string
                file_count, file_size = get_bucket_stats(bucket, bucket_prefix)
                writer_object.writerow([date_string, bucket_dict[bucket_name]['vendor_id'], file_count, file_size])
            print(f'Data Inflow for {date_string} complete...')
        f_object.close()
    dfp = read_data(f'data-inflows-{end}-to-{start}.csv')
    warnings, alerts = generate_vendor_messages(dfp)
    graph_file = generate_area_graph(dfp, f'data-inflows-{end}-to-{start}.csv')
    send_mail(
        'worthy71@anomalysix.com',
        ['worthy71@anomalysix.com'], # 'w71@anomalysix.com'
        f'Data Inflows For {start}',
        '',
        '',
        graph_file,
        files=[f'data-inflows-{end}-to-{start}.csv']
    )