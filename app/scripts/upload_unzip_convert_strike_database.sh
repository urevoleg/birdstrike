cd ../src
wget -O database.zip https://wildlife.faa.gov/assets/database.zip && unzip -o database.zip && mdb-export Public.accdb STRIKE_REPORTS > strike_reports.csv