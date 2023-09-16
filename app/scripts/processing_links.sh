!/bin/bash
#YEAR = $1

cd data

while read file; do
  year="$(echo $file | cut -d '/' -f 7)"
  filename="$(echo $file | cut -d '/' -f 8)"
  # echo "Downloading....."
  wget -nv --show-progress -O "$year-$filename" $file
  python ../load.py $year-$filename
done < "../weather_data_links.txt"

# delete all download files
rm *.csv *.sql
