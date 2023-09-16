!/bin/bash
cd data
while read file; do
    wget ${file} -b && rm wget-log*
done < ../weather_data_links.txt