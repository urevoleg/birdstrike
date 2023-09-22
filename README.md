# BIRDSTRIKE
![birdstrike.png](app%2Fimg%2Fbirdstrike.png)

## Infra

[docker-compose.yaml](docker-compose.yaml)

## EL

### 1. Файл с БД столкновений доступен по адресу `https://wildlife.faa.gov/assets/database.zip`

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

### 2. Файл с метеостанциями доступен по адресу `https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv`

Здесь подход такой же: файл еще меньше чем база столкновений, поэтому загружаем чем хотим, в базу грузим также любым удобным способом
У меня скачивание при помощи bash-скрипта [upload_meteostations_list.sh](app%2Fscripts%2Fupload_meteostations_list.sh), processing и загрузка в бд на pandas+sqlalchemy:
- фильтруются станции по периоду активности (должны быть активным после 2018 года)
- дропаются станции без координат - их ни к чему приджойнить невозможно

Соответственно данные можно перегружать хоть каждый день =)

### 3. Download weather data

Не стал использовать API, пошел путем загрузки файлов для каждой метеостанции за необходимый год - они доступны через http.

В бд создал необходимую структуру таблицы (только необходимые поля для аналитиков): это упрощает немного реализацию загрузки, тк
во всех файлах есть эти поля, а вот с остальными есть проблемы (могут отсутствовать случайные поля).
На python формирую список ссылок [weather_data_links.txt](app%2Fsrc%2Fweather_data_links.txt) для заданного года (python processing.py 2023).
На python реализована загрузка данных в raw слой (load.py)

Чтение ссылок, скачивание файлов и их загрузка в бд происходит в bash-скрипте [processing_links.sh](app%2Fscripts%2Fprocessing_links.sh)
![processing_links_sh.png](app%2Fimg%2Fprocessing_links_sh.png)

Из ссылки извлекается год и название файла, wget выкачивает файл, python load.py загружает в бд и записывает в сервисную таблицу инфо, что данные для такого-то файла 
загружены.


## T-transform

### 1. Find nearest meteostation

Здесь без прикрас, всё выполняется самой субд [calculate_near_weatherstation.sql](app%2Fsql%2Fraw%2Fcalculate_near_weatherstation.sql)

Естественно нужно выполнять это действие на для каждого strike, а для каждого уникального аэропорта и каждый уникальной станции - получаем
cross-join + row_number и выбираем там где rn=1. И кладем в таблицу, если нужно добавлять в таблицу, то запрос дополняется не хитрым условием

Тут можно глянуть план запроса:

![explain_nearest_meteo.png](app%2Fimg%2Fexplain_nearest_meteo.png)

Обращает на себя внимание:
- Nested Loop и SeqScan - будет долго, посмотрим насколько

![timeit_nearest_meteo.png](app%2Fimg%2Ftimeit_nearest_meteo.png)

⏱ Выполнение, около 2-х минут (приемлимо) - точно не часы.

Кстати есть отличный [сервис](https://demo-explain.tensor.ru/plan/) для раскуривания планов запроса. 
Статьи:
- [Понимаем планы PostgreSQL-запросов еще удобнее](https://habr.com/ru/companies/tensor/articles/505348/)
- [Рецепты для хворающих SQL-запросов](https://habr.com/ru/companies/tensor/articles/492694/)


### Data Quality

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

### 2. Добавляем погоду к strike

![postgres-muscle.png](app%2Fimg%2Fpostgres-muscle.png)


СУБД - это наша сила. Данные уже у нас в базе, можно использовать любой удобный способ объединения данных. 
Точно следует попробовать сделать это средствами СУБД, для этого потребуется сформировать полную дату инцидента для `strike_report` - это можно делать 
в момент переноса из `raw -> stg`

Создать нужное поле `incidented_at` можно при помощи трехэтажного выражения =)

```sql
to_timestamp(concat("INCIDENT_DATE"::date::TEXT, ' ', "TIME"), 'YYYY-MM-DD HH24:MI') AT TIME ZONE 'UTC'
```

Также следует обратить внимание, что инциденты могут происходить в любое время, например, 13:31 или 00:05, а погода у нас за каждый час.
Процесс поиска ближайщего значения может быть долгим (привет, Nested Loop), попробовать обойти этот момент можно так:

Округлить дату инцидента до ближайщего часа:

```sql
date_trunc('hour', to_timestamp(concat("INCIDENT_DATE"::date::TEXT, ' ', "TIME"), 'YYYY-MM-DD HH24:MI') AT TIME ZONE 'UTC' + INTERVAL '30minute')
```

И объединять данные по идентификатору стации, округленной дате инцидента + дата погоды. Для ускорения можно создать индексы на 
соответствующие поля (по дате погоды `DATE` и округленной дате инцидента). 

ps: JOIN таблиц будет быстрее если преобразования столбцов по которым идет JOIN выполнять не во время JOIN, а выполнить до:
то есть при загрузке таблицы столкновений в stg-layer делать очистку данных по корректному времени, формировать столбец с полной датой 
инцидента и столбец с округленной датой инцидента.


# TODO

Стоит отметить:
- за каждый год получается около 14000 файлов (не все их них существуют, но пусть будет оценка сверху), 
за 6 неполных лет будет 84000 файлов. Положим, что за 2023 год самые полные данные, вес одного файла составляет от единиц до десятков МВ.
Возьмем что-то среднее, 15МВ. Получается крайне много и самое главное бОльшая часть из этих данных будет не нужна, тк инциденты не происходят
каждый час.

Что можно предпринять:
- можно не тащить все данные всех метеостанций, а взять только те, что нужны. В таблице связей всего 2600 метеостанций - почти в 7 раз меньше.
Но всё равно довольно много: для 1830 станций размер таблицы погода составляет ~5.8GB за 2018 год.

За 2018 год инцидентов - 16202, записей погоды для всех метеостанций 32092318 - меньше 0.05% - однозначно нужно пользовать API ✅

- использование API: запрашивать погоду для каждой пары инцидент-метеостация в диапазоне +-30мин.
Вызовов API будет около 70000 (с 2018 года по нв инцидентов 70919)
- можно объединить инциденты и метеостанции, сгруппировать по метеостанции и посчитать мин\макс дату инцидента, соответственно за этот период и
нужно загрузить данные. Для части станций (около 500 данные нужны всего лишь за год)

# API

Добавил работу с API:

Генератор, реализованный [meteostation_with_incidented_at.sql](app%2Fsql%2Fraw%2Fmeteostation_with_incidented_at.sql) выдаёт пары инцидент и метеостанция,
период времени формируется в диапазоне +-30мин от инцидента на SQL:

```python
    def gen_incident_chunk():
        engine = create_engine(os.getenv('SQLALCHEMY_DATABASE_URI'))

        with engine.connect() as con:
            stmt = """SELECT max(raw_incidented_at) FROM raw.weather_noaa;"""
            dated_at = con.execute(stmt).fetchone()[0]

            with open('sql/raw/meteostation_with_incidented_at.sql', 'r') as f:
                stmt = text(f.read())
            rows = con.execute(stmt,
                               dated_at=dated_at,)
            for row in rows:
                yield row
```

Далее идет подготовка запроса к API и его выполнение, тк API периодически выдают ошибки, настроены несколько попыток обращения (3):
```python
def retries(func):
    def wrapper(*args, **kwargs):
        for i in range(3):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                pass
        return {}
    return wrapper
```

Ответы от API сортируются по близости к дате события, выбирается ближайщее:

```python
near_row = sorted(response_data, key=lambda x: parse(x['DATE']) - row.raw_incidented_at)[0]
```

Также, работа с API организована как batch processing (формируются chunk, для каждого chunk собираются ответы от API и весь batch
кладется в БД).

### TODO

Вот этот кусочек:
```python
        for row in chunk_rows:
            response_data = fetch_data_from_noaa(**row_handler(row))
            if not response_data == []:
                near_row = sorted(response_data, key=lambda x: parse(x['DATE']) - row.raw_incidented_at)[0]
            else:
                near_row = {}
            output += [{**row._asdict(), 'json_data': json.dumps(near_row, default=str)}]
```

Можно и нужно завернуть в ThreadPoolExecutor - чтобы хоть какая-то параллельность была =)

# Pipeline

```
upload_unzip_convert_strike_database.sh ->\
upload_meteostations_list.sh ->\
psql -h db -p 5432 -U pgadmin -w -d birdstrike -f sql/migrations/raw.sql ->\
python3 processing.py strike_isd -> \
python3 load.py
```

