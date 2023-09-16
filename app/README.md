# BIRDSTRIKE
![birdstrike.png](img%2Fbirdstrike.png)

## Infra

postgres [docker-compose.yaml](docker-compose.yaml)

## EL

1. Файл с БД столкновений доступен по адресу `https://wildlife.faa.gov/assets/database.zip`

Узнать это можно через DevTools (F12 в браузере):
![link_for_faa_database.png](img%2Flink_for_faa_database.png)

Далее можно действовать так:
 - download\unzip скрипт на bash (пример можно найти  [upload_unzip_convert_strike_database.sh](upload_unzip_convert_strike_database.sh)
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
У меня скачивание при помощи bash-скрипта [upload_meteostations_list.sh](upload_meteostations_list.sh), processing и загрузка в бд на pandas+sqlalchemy:
- фильтруются станции по периоду активности (должны быть активным после 2018 года)
- дропаются станции без координат - их ни к чему приджойнить невозможно

Соответственно данные можно перегружать хоть каждый день =)

3. Download weather data

Не стал использовать API, пошел путем загрузки файлов для каждой метеостанции за необходимый год - они доступны через http.

В бд создал необходимую структуру таблицы (только необходимые поля для аналитиков): это упрощает немного реализацию загрузки, тк
во всех файлах есть эти поля, а вот с остальными есть проблемы (могут отсутствовать случайные поля).
На python формирую список ссылок [weather_data_links.txt](weather_data_links.txt) для заданного года (python processing.py 2023).
На python реализована загрузка данных в raw слой (load.py)

Чтение ссылок, скачивание файлов и их загрузка в бд происходит в bash-скрипте [processing_links.sh](processing_links.sh)
![processing_links_sh.png](img%2Fprocessing_links_sh.png)

Из ссылки извлекается год и название файла, wget выкачивает файл, python load.py загружает в бд и записывает в сервисную таблицу инфо, что данные для такого-то файла 
загружены.

ps: итого обработано 594 файла, объем загруженных данных в бд указан на скрине ниже

![raw_table_sizes.png](img%2Fraw_table_sizes.png)

## T-transform

1. Find nearest meteostation

Здесь без прикрас, всё выполняется самой субд [calculate_near_weatherstation.sql](sql%2Fraw%2Fcalculate_near_weatherstation.sql)

Естественно нужно выполнять это действие на для каждого strike, а для каждого уникального аэропорта и каждый уникальной станции - получаем
cross-join + row_number и выбираем там где rn=1. И кладем в таблицу, если нужно добавлять в таблицу, то запрос дополняется не хитрым условием

Тут можно глянуть план запроса:
![explain_nearest_meteostation.png](img%2Fexplain_nearest_meteostation.png)

Обращает на себя внимание:
- время выполнения 1сек
- пишем и читаем на диск (Можно увеличить память сессии ```sql SET work_mem = '128MB';```)

Кстати есть отличный [сервис](https://demo-explain.tensor.ru/plan/) для раскуривания планов запроса. 
Статьи:
- [Понимаем планы PostgreSQL-запросов еще удобнее](https://habr.com/ru/companies/tensor/articles/505348/)
- [Рецепты для хворающих SQL-запросов](https://habr.com/ru/companies/tensor/articles/492694/)

2. JOIN погоды к strike

Аналогично, всё сделает СУБД - это наша сила.
![postgres-muscle.png](img%2Fpostgres-muscle.png)

SQL query - [join_strike_weather.sql](sql%2Fods%2Fjoin_strike_weather.sql)





