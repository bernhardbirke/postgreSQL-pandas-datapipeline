### Backup PostgreSQL File

Run the following command in the shell:
pg_dump -U username -O file_data -f file_data.sql

or simply:
pg_dump file_data > file_data.sql

This saves the file in the current directory of the shell.

Then copy the file to another server and run:

psql -U username_data -d database -f file_data.sql

### Necessary Modules

psycopg2
pip install psycopg2
pip install SQLAlchemy
pip install pandas

### Schedule the batch update

To run the smartmeter_ETL.py file on schedule, a shell file (getdata.sh) is created. Always using full path directories here (including /home/username/etc..).

```
#!/bin/bash
#activate virtual environment
source "/full/path/to/project/venv/bin/activate"

#move to the project directory
cd /full/path/to/project/

#start python-file
python filename.py
```

Schedule the python file in crontab using the following line in `crontab -e`:

```
 */10 * * * * /full/path/to/shfile/getdata.sh
```

To check if cron is running properly, run `grep CRON /var/log/syslog`.

This crontab runs the `getdata.sh`-file every 10 minutes every day.

### configure Grafana

Make Grafana accessible without any login required by enabling anonymous access in the configuration file. (see also: https://grafana.com/docs/grafana/latest/setup-grafana/configure-security/configure-authentication/grafana/#settings)

```
#################################### Anonymous Auth ######################
[auth.anonymous]
# enable anonymous access
enabled = true

# specify organization name that should be used for unauthenticated users
org_name = Main Org.

# specify role for unauthenticated users
org_role = Viewer

# mask the Grafana version number for unauthenticated users
hide_version = true

# set to true if you want to allow browsers to render Grafana in a <frame>, <iframe>, <embed> or <object>. default is false.
allow_embedding = true


```

#### Create Role to access data from grafana

using the following psql commands

```
CREATE ROLE grafana
LOGIN
PASSWORD 'safe_password_123';

GRANT pg_read_all_data TO grafana;
```
