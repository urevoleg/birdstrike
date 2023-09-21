# BIRDSTRIKE
![birdstrike.png](app%2Fimg%2Fbirdstrike.png)

## Infra

[docker-compose.yaml](docker-compose.yaml)

## EL

1. Файл с БД столкновений доступен по адресу `https://wildlife.faa.gov/assets/database.zip`

Узнать это можно через DevTools (F12 в браузере):
![link_for_faa_database.png](app%2Fimg%2Flink_for_faa_database.png)

Далее можно действовать так:
 - download\unzip скрипт на bash (пример можно найти  [upload_unzip_convert_strike_database.sh](app%2Fscripts%2Fupload_unzip_convert_strike_database.sh)
 - нашел такой tool для работы с MSAccess [mdbtools](https://github.com/mdbtools/mdbtools/tree/dev)
он как раз необходим для чтения .accdb на *nix системах. При помощи него можно сделать экспорт таблицы в CSV или SQL скрипт для Postgres
у меня реализован CSV

```
# csv
mdb-export Public.accdb STRIKE_REPORTS > strike_reports.csv

# sql
mdb-export -I postgres Public.accdb STRIKE_REPORTS > strike_reports.sql
```

 - upload csv to db raw with pandas (или как альтернатива при экспорте в *.sql использовать psql для его выполнения)

База столкновений маленькая (~150МВ в разжатом виде) поэтому её можно перегружать полностью или использовать что-нибудь типа pandas\sqlalchemy
или psql совместно с mdb-sql для решения задачи инкрементальной загрузки

2. Файл с метеостанциями доступен по адресу `https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv`

Здесь подход такой же: файл еще меньше чем база столкновений, поэтому загружаем чем хотим, в базу грузим также любым удобным способом
У меня скачивание при помощи bash-скрипта [upload_meteostations_list.sh](app%2Fscripts%2Fupload_meteostations_list.sh), processing и загрузка в бд на pandas+sqlalchemy:
- фильтруются станции по периоду активности (должны быть активным после 2018 года)
- дропаются станции без координат - их ни к чему приджойнить невозможно

Соответственно данные можно перегружать хоть каждый день =)

3. Download weather data

Не стал использовать API, пошел путем загрузки файлов для каждой метеостанции за необходимый год - они доступны через http.

В бд создал необходимую структуру таблицы (только необходимые поля для аналитиков): это упрощает немного реализацию загрузки, тк
во всех файлах есть эти поля, а вот с остальными есть проблемы (могут отсутствовать случайные поля).
На python формирую список ссылок [weather_data_links.txt](app%2Fsrc%2Fweather_data_links.txt) для заданного года (python processing.py 2023).
На python реализована загрузка данных в raw слой (load.py)

Чтение ссылок, скачивание файлов и их загрузка в бд происходит в bash-скрипте [processing_links.sh](app%2Fscripts%2Fprocessing_links.sh)
![processing_links_sh.png](app%2Fimg%2Fprocessing_links_sh.png)

Из ссылки извлекается год и название файла, wget выкачивает файл, python load.py загружает в бд и записывает в сервисную таблицу инфо, что данные для такого-то файла 
загружены.

ps: итого обработано 594 файла, объем загруженных данных в бд указан на скрине ниже

![raw_table_sizes.png](app%2Fimg%2Fraw_table_sizes.png)

## T-transform

1. Find nearest meteostation

Здесь без прикрас, всё выполняется самой субд [calculate_near_weatherstation.sql](app%2Fsql%2Fraw%2Fcalculate_near_weatherstation.sql)

Естественно нужно выполнять это действие на для каждого strike, а для каждого уникального аэропорта и каждый уникальной станции - получаем
cross-join + row_number и выбираем там где rn=1. И кладем в таблицу, если нужно добавлять в таблицу, то запрос дополняется не хитрым условием

Тут можно глянуть план запроса:

![explain_nearest_meteo.png](app%2Fimg%2Fexplain_nearest_meteo.png)

Обращает на себя внимание:
- Nested Loop и SeqScan - будет долго, посмотрим насколько
- 
![timeit_nearest_meteo.png](app%2Fimg%2Ftimeit_nearest_meteo.png)

⏱ Выполнение, около 2-х минут (приемлимо) - точно не часы.

Кстати есть отличный [сервис](https://demo-explain.tensor.ru/plan/) для раскуривания планов запроса. 
Статьи:
- [Понимаем планы PostgreSQL-запросов еще удобнее](https://habr.com/ru/companies/tensor/articles/505348/)
- [Рецепты для хворающих SQL-запросов](https://habr.com/ru/companies/tensor/articles/492694/)

2. Data Quality

![data_quality.png](app%2Fimg%2Fdata_quality.png)

Качество можно проверять долго и нудно, будем исходить из последующего объединения данных по времени. 

Нам необходимо, чтобы дата и время были в нужном формате:
- дата `INCIDENT_DATE` - `%Y-%m-%d`
- время `TIME` - `%HH:%MM`

Для SQL это может выглядеть не так приятно, как в Python. Например, запрос ниже выведет все строки, где время не совпадает с указанным pattern:

```sql
SELECT "INDEX_NR",
	   "INCIDENT_DATE",
	   "TIME"
FROM raw.strike_reports sr 
WHERE "TIME" NOT SIMILAR TO '%((0|1)(0|1|2|3|4|5|6|7|8|9):(0|1|2|3|4|5)(0|1|2|3|4|5|6|7|8|9))|2(0|1|2|3):(0|1|2|3|4|5)(0|1|2|3|4|5|6|7|8|9)%';
```

Результат:

![dq_time_strike_report_1.png](app%2Fimg%2Fdq_time_strike_report_1.png)

![dq_time_strike_report_2.png](app%2Fimg%2Fdq_time_strike_report_2.png)

475 некорректных записи со временем - они не должны пройти в следующий layer, но и ничего не делать с ними нельзя.
Их можно складывать в отдельную таблицу - чтобы понимать, какое качество данных мы передаем дальше.

3. Добавляем погоду к strike

![postgres-muscle.png](app%2Fimg%2Fpostgres-muscle.png)


СУБД - это наша сила. Данные уже у нас в базе, можно использовать любой удобный способ объединения данных. 
Точно следует попробовать сделать это средствами СУБД, для этого потребуется сформировать полную дату инцидента для `strike_report` - это можно делать 
в момент переноса из `raw -> stg`

Создать нужное поле `incidented_at` можно при помощи трехэтажного выражения =)

```sql
to_timestamp(concat("INCIDENT_DATE"::date::TEXT, ' ', "TIME"), 'YYYY-MM-DD HH24:MI') AT TIME ZONE 'UTC'
```
