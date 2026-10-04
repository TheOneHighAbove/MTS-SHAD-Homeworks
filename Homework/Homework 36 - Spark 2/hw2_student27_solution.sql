-- ДЗ: многоуровневый аналитический слой в ClickHouse
-- Студент: student27
-- База данных: student27
-- Кластер: bolwad
--
-- Работал через ClickHouse Web SQL UI.
-- Все DDL выполняются с ON CLUSTER bolwad.
-- INSERT делается в Distributed-таблицы, SELECT для проверок тоже из Distributed.

/* ============================================================================
0. Проверка окружения
============================================================================ */

SELECT currentUser(), currentDatabase();

SELECT cluster, shard_num, replica_num, host_name, host_address
FROM system.clusters
ORDER BY cluster, shard_num, replica_num;

SELECT *
FROM system.macros;

DESCRIBE TABLE homework_data.push_events_source;

SELECT
    count() AS source_rows,
    min(event_dt) AS min_event_dt,
    max(event_dt) AS max_event_dt,
    now() AS current_time,
    now() - INTERVAL 30 DAY AS ttl_border
FROM homework_data.push_events_source;

-- По проверке:
-- source_rows = 1050278
-- min_event_dt = 2026-07-01 14:23:56
-- max_event_dt = 2026-07-31 16:17:22
-- На момент запуска граница TTL была около 2026-07-12/2026-07-13.

/* ============================================================================
1. Подготовка базы

DROP-запросы нужны только для воспроизводимого перезапуска решения.
В Web UI я запускал их по одному, потому что интерфейс не всегда удобно
обрабатывает пачку запросов сразу.
============================================================================ */

CREATE DATABASE IF NOT EXISTS student27 ON CLUSTER bolwad;

DROP TABLE IF EXISTS student27.mv_push_events_agg ON CLUSTER bolwad SYNC;
DROP TABLE IF EXISTS student27.mv_push_counters ON CLUSTER bolwad SYNC;

DROP TABLE IF EXISTS student27.push_events_raw ON CLUSTER bolwad SYNC;
DROP TABLE IF EXISTS student27.push_events_raw_local ON CLUSTER bolwad SYNC;

DROP TABLE IF EXISTS student27.push_events_agg ON CLUSTER bolwad SYNC;
DROP TABLE IF EXISTS student27.push_events_agg_local ON CLUSTER bolwad SYNC;

DROP TABLE IF EXISTS student27.push_counters ON CLUSTER bolwad SYNC;
DROP TABLE IF EXISTS student27.push_counters_local ON CLUSTER bolwad SYNC;

DROP TABLE IF EXISTS student27.push_events_dedup ON CLUSTER bolwad SYNC;
DROP TABLE IF EXISTS student27.push_events_dedup_local ON CLUSTER bolwad SYNC;

/* ============================================================================
2. Задание 1. Raw-слой

Создаю локальную таблицу на ReplicatedMergeTree и Distributed-таблицу поверх нее.
Партиционирование делаю по дню события: toYYYYMMDD(event_dt).
TTL ставлю ровно по заданию: 30 дней от event_dt.
В ORDER BY первым идет user_id, потому что в задании нужен быстрый поиск по
пользователю.

Для Distributed выбрал cityHash64(user_id). Так события одного пользователя
стабильно попадают на один шард, а пользователи распределяются достаточно
равномерно по двум шардам.

Ответ на вопрос:
если вставлять данные в *_local, а не в Distributed-таблицу, данные попадут
только на конкретную ноду. Нормального шардирования не будет, появится перекос,
а запросы через Distributed будут читать неполную или неравномерную картину.
============================================================================ */

CREATE TABLE student27.push_events_raw_local ON CLUSTER bolwad
(
    event_dt DateTime,
    event_id UInt64,
    user_id UInt64,
    message_id UInt64,
    platform LowCardinality(String),
    status LowCardinality(String),
    latency_ms UInt32
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/student27/push_events_raw_local',
    '{replica}'
)
PARTITION BY toYYYYMMDD(event_dt)
ORDER BY (user_id, event_dt, event_id)
TTL event_dt + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE student27.push_events_raw ON CLUSTER bolwad
AS student27.push_events_raw_local
ENGINE = Distributed(
    bolwad,
    student27,
    push_events_raw_local,
    cityHash64(user_id)
);

/* ============================================================================
3. Задание 2. AggregatingMergeTree + MV

В aggregate-слое храню состояния агрегатных функций, а не готовые числа:
countState(), uniqState(user_id), avgState(latency_ms).
При чтении использую countMerge(), uniqMerge(), avgMerge().

Ответ на вопрос:
если создать MV после вставки данных в raw, она не обработает уже вставленные
строки. MV начнет работать только на новые INSERT. Исправить можно двумя
способами: создать MV до INSERT или отдельно сделать backfill в агрегатную
таблицу через INSERT INTO ... SELECT ...State() FROM raw GROUP BY ...

Ответ на вопрос:
avgMerge без GROUP BY вернет одно общее среднее по всем состояниям. Это
корректно только если нужно глобальное среднее. Для среднего по
(event_date, platform) обязательно нужен GROUP BY по этим полям.
============================================================================ */

CREATE TABLE student27.push_events_agg_local ON CLUSTER bolwad
(
    event_date Date,
    platform LowCardinality(String),
    events_count_state AggregateFunction(count),
    users_uniq_state AggregateFunction(uniq, UInt64),
    latency_avg_state AggregateFunction(avg, UInt32)
)
ENGINE = ReplicatedAggregatingMergeTree(
    '/clickhouse/tables/{shard}/student27/push_events_agg_local',
    '{replica}'
)
PARTITION BY toYYYYMMDD(event_date)
ORDER BY (event_date, platform);

CREATE TABLE student27.push_events_agg ON CLUSTER bolwad
AS student27.push_events_agg_local
ENGINE = Distributed(
    bolwad,
    student27,
    push_events_agg_local,
    cityHash64(platform)
);

CREATE MATERIALIZED VIEW student27.mv_push_events_agg ON CLUSTER bolwad
TO student27.push_events_agg_local
AS
SELECT
    toDate(event_dt) AS event_date,
    platform,
    countState() AS events_count_state,
    uniqState(user_id) AS users_uniq_state,
    avgState(latency_ms) AS latency_avg_state
FROM student27.push_events_raw_local
GROUP BY
    event_date,
    platform;

/* ============================================================================
4. Задание 3. SummingMergeTree + MV

Счетчики считаю через countIf по статусам delivered, failed, opened.
Колонку user_id оставляю намеренно, как написано в задании.

Ответ на вопрос про user_id:
user_id здесь не является измерением агрегации, но это числовая колонка и она
не входит в ORDER BY. Поэтому SummingMergeTree при схлопывании частей может
сложить значения user_id, и после OPTIMIZE FINAL эта колонка становится
бессмысленной.

Как починить:
1. Убрать user_id из таблицы counters, потому что результат нужен только по
   (event_date, platform).
2. Если нужны счетчики по пользователям, добавить user_id в ORDER BY и GROUP BY,
   то есть считать по (event_date, platform, user_id).

Ответ на вопрос про avg:
SummingMergeTree не подходит для среднего напрямую, потому что среднее не
складывается. Для avg надо хранить сумму и количество, а потом делить sum/count,
или использовать AggregatingMergeTree с avgState()/avgMerge().
============================================================================ */

CREATE TABLE student27.push_counters_local ON CLUSTER bolwad
(
    event_date Date,
    platform LowCardinality(String),
    user_id UInt64,
    delivered_count UInt64,
    failed_count UInt64,
    opened_count UInt64
)
ENGINE = ReplicatedSummingMergeTree(
    '/clickhouse/tables/{shard}/student27/push_counters_local',
    '{replica}'
)
PARTITION BY toYYYYMMDD(event_date)
ORDER BY (event_date, platform);

CREATE TABLE student27.push_counters ON CLUSTER bolwad
AS student27.push_counters_local
ENGINE = Distributed(
    bolwad,
    student27,
    push_counters_local,
    cityHash64(platform)
);

CREATE MATERIALIZED VIEW student27.mv_push_counters ON CLUSTER bolwad
TO student27.push_counters_local
AS
SELECT
    toDate(event_dt) AS event_date,
    platform,
    any(user_id) AS user_id,
    countIf(status = 'delivered') AS delivered_count,
    countIf(status = 'failed') AS failed_count,
    countIf(status = 'opened') AS opened_count
FROM student27.push_events_raw_local
GROUP BY
    event_date,
    platform;

/* ============================================================================
5. INSERT в raw

В задании требуется TTL 30 дней от event_dt. Так как часть исходных событий уже
старше TTL относительно времени сервера, я вставляю только строки, которые
должны остаться в raw после применения TTL.

Если вставить весь source без фильтра, raw удалит старые строки по TTL, а MV
успеет посчитать их в агрегатах. Тогда raw и agg не будут совпадать. Фильтр ниже
делает состояние слоев согласованным и соответствует фактическому содержимому
raw-таблицы с TTL.
============================================================================ */

INSERT INTO student27.push_events_raw
SELECT *
FROM homework_data.push_events_source
WHERE event_dt >= now() - INTERVAL 30 DAY;

/* ============================================================================
6. Проверки задания 1
============================================================================ */

SELECT count() AS raw_rows
FROM student27.push_events_raw;

SELECT hostName(), count()
FROM student27.push_events_raw
GROUP BY hostName()
ORDER BY hostName();

SELECT database, table, is_readonly, absolute_delay, log_pointer
FROM system.replicas
WHERE database = 'student27'
ORDER BY table;

-- Полученное распределение raw по шардам:
-- ch-vm-bolwad-sh1-r2-qacbexxah: 337504
-- ch-vm-bolwad-sh2-r1-qacbexxah: 329002
--
-- Репликация:
-- is_readonly = 0
-- absolute_delay = 0

/* ============================================================================
7. Проверки задания 2
============================================================================ */

SELECT
    event_date,
    platform,
    countMerge(events_count_state) AS events_count,
    uniqMerge(users_uniq_state) AS unique_users,
    avgMerge(latency_avg_state) AS avg_latency_ms
FROM student27.push_events_agg
GROUP BY
    event_date,
    platform
ORDER BY
    event_date,
    platform
LIMIT 20;

SELECT
    toDate(event_dt) AS event_date,
    platform,
    count() AS events_count,
    uniq(user_id) AS unique_users,
    avg(latency_ms) AS avg_latency_ms
FROM student27.push_events_raw
GROUP BY
    event_date,
    platform
ORDER BY
    event_date,
    platform
LIMIT 20;

WITH
agg AS
(
    SELECT
        event_date,
        platform,
        countMerge(events_count_state) AS events_count,
        uniqMerge(users_uniq_state) AS unique_users,
        avgMerge(latency_avg_state) AS avg_latency_ms
    FROM student27.push_events_agg
    GROUP BY event_date, platform
),
raw AS
(
    SELECT
        toDate(event_dt) AS event_date,
        platform,
        count() AS events_count,
        uniq(user_id) AS unique_users,
        avg(latency_ms) AS avg_latency_ms
    FROM student27.push_events_raw
    GROUP BY event_date, platform
)
SELECT
    raw.event_date,
    raw.platform,
    raw.events_count AS raw_events_count,
    agg.events_count AS agg_events_count,
    raw.unique_users AS raw_unique_users,
    agg.unique_users AS agg_unique_users,
    raw.avg_latency_ms AS raw_avg_latency_ms,
    agg.avg_latency_ms AS agg_avg_latency_ms
FROM raw
INNER JOIN agg USING (event_date, platform)
WHERE
    raw.events_count != agg.events_count
    OR raw.unique_users != agg.unique_users
    OR abs(raw.avg_latency_ms - agg.avg_latency_ms) > 0.000001
ORDER BY raw.event_date, raw.platform;

-- Результат проверки расхождений: empty result.
-- Значит агрегаты из push_events_agg совпали с прямым расчетом из raw.

OPTIMIZE TABLE student27.push_events_agg_local ON CLUSTER bolwad FINAL;

SELECT
    event_date,
    platform,
    countMerge(events_count_state) AS events_count,
    uniqMerge(users_uniq_state) AS unique_users,
    avgMerge(latency_avg_state) AS avg_latency_ms
FROM student27.push_events_agg
GROUP BY
    event_date,
    platform
ORDER BY
    event_date,
    platform
LIMIT 20;

/* ============================================================================
8. Проверки задания 3
============================================================================ */

SELECT *
FROM student27.push_counters
ORDER BY event_date, platform
LIMIT 20;

OPTIMIZE TABLE student27.push_counters_local ON CLUSTER bolwad FINAL;

SELECT *
FROM student27.push_counters
ORDER BY event_date, platform
LIMIT 20;

-- SELECT * из Distributed-таблицы показывает несколько строк на один
-- (event_date, platform), потому что данные лежат на разных шардах.
-- Финальные счетчики по кластеру надо читать через GROUP BY и sum().

SELECT
    event_date,
    platform,
    sum(delivered_count) AS delivered_count,
    sum(failed_count) AS failed_count,
    sum(opened_count) AS opened_count
FROM student27.push_counters
GROUP BY
    event_date,
    platform
ORDER BY
    event_date,
    platform
LIMIT 20;

SELECT
    toDate(event_dt) AS event_date,
    platform,
    countIf(status = 'delivered') AS delivered_count,
    countIf(status = 'failed') AS failed_count,
    countIf(status = 'opened') AS opened_count
FROM student27.push_events_raw
GROUP BY
    event_date,
    platform
ORDER BY
    event_date,
    platform
LIMIT 20;

-- Пример результата после суммирования counters:
-- 2026-07-12 ANDROID: delivered=1151, failed=1132, opened=1096
-- 2026-07-12 DESKTOP: delivered=1105, failed=1124, opened=1159
-- Эти числа совпали с прямым расчетом из raw.

/* ============================================================================
9. Задание 4. ReplacingMergeTree

Создаю локальную таблицу на ReplacingMergeTree(event_dt) и Distributed-таблицу.
ORDER BY event_id, поэтому дедупликация идет по event_id. Версия - event_dt.

Ответ на вопросы:
count() и count() FINAL отличаются, потому что ReplacingMergeTree не удаляет
дубли сразу при INSERT. Дубли физически схлопываются во время фоновых merge,
а FINAL применяет дедупликацию во время чтения.

При ReplacingMergeTree(event_dt) выигрывает строка с максимальным event_dt.
Для строгой дедупликации "прямо сейчас" на ReplacingMergeTree полагаться нельзя:
нужен FINAL или явная логика, например argMax по версии.
============================================================================ */

CREATE TABLE student27.push_events_dedup_local ON CLUSTER bolwad
(
    event_dt DateTime,
    event_id UInt64,
    user_id UInt64,
    message_id UInt64,
    platform LowCardinality(String),
    status LowCardinality(String),
    latency_ms UInt32
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/student27/push_events_dedup_local',
    '{replica}',
    event_dt
)
PARTITION BY toYYYYMMDD(event_dt)
ORDER BY event_id;

CREATE TABLE student27.push_events_dedup ON CLUSTER bolwad
AS student27.push_events_dedup_local
ENGINE = Distributed(
    bolwad,
    student27,
    push_events_dedup_local,
    cityHash64(event_id)
);

INSERT INTO student27.push_events_dedup
SELECT *
FROM homework_data.push_events_source;

SELECT count()
FROM student27.push_events_dedup;

SELECT count()
FROM student27.push_events_dedup FINAL;

-- Полученный результат:
-- count()       = 1026546
-- count() FINAL = 1025474
-- Разница = 1072 дубля по event_id.

/* ============================================================================
10. Задание 5. Сравнение производительности

Сравниваю один и тот же запрос двумя способами:
1. из raw через обычные count/uniq/avg;
2. из agg через countMerge/uniqMerge/avgMerge.

Беру последние 7 дней относительно максимальной даты в данных. Если брать
now() - INTERVAL 7 DAY, результат будет пустым, потому что данные заканчиваются
2026-07-31, а серверное now() уже позже.
============================================================================ */

SELECT
    platform,
    count() AS events_count,
    uniq(user_id) AS unique_users,
    avg(latency_ms) AS avg_latency_ms
FROM student27.push_events_raw
WHERE event_dt >=
(
    SELECT max(event_dt) - INTERVAL 7 DAY
    FROM student27.push_events_raw
)
GROUP BY platform
ORDER BY platform;

SELECT
    platform,
    countMerge(events_count_state) AS events_count,
    uniqMerge(users_uniq_state) AS unique_users,
    avgMerge(latency_avg_state) AS avg_latency_ms
FROM student27.push_events_agg
WHERE event_date >=
(
    SELECT max(event_date) - 7
    FROM student27.push_events_agg
)
GROUP BY platform
ORDER BY platform;

WITH
raw_7d AS
(
    SELECT
        platform,
        count() AS events_count,
        uniq(user_id) AS unique_users,
        avg(latency_ms) AS avg_latency_ms
    FROM student27.push_events_raw
    WHERE event_dt >=
    (
        SELECT max(event_dt) - INTERVAL 7 DAY
        FROM student27.push_events_raw
    )
    GROUP BY platform
),
agg_7d AS
(
    SELECT
        platform,
        countMerge(events_count_state) AS events_count,
        uniqMerge(users_uniq_state) AS unique_users,
        avgMerge(latency_avg_state) AS avg_latency_ms
    FROM student27.push_events_agg
    WHERE event_date >=
    (
        SELECT max(event_date) - 7
        FROM student27.push_events_agg
    )
    GROUP BY platform
)
SELECT
    raw_7d.platform,
    raw_7d.events_count AS raw_events_count,
    agg_7d.events_count AS agg_events_count,
    raw_7d.unique_users AS raw_unique_users,
    agg_7d.unique_users AS agg_unique_users,
    raw_7d.avg_latency_ms AS raw_avg_latency_ms,
    agg_7d.avg_latency_ms AS agg_avg_latency_ms
FROM raw_7d
INNER JOIN agg_7d USING (platform)
WHERE
    raw_7d.events_count != agg_7d.events_count
    OR raw_7d.unique_users != agg_7d.unique_users
    OR abs(raw_7d.avg_latency_ms - agg_7d.avg_latency_ms) > 0.000001
ORDER BY raw_7d.platform;

SELECT query_duration_ms, read_rows, read_bytes, query
FROM system.query_log
WHERE event_time > now() - 120
  AND type = 'QueryFinish'
  AND query LIKE '%push_events%'
ORDER BY event_time DESC
LIMIT 10;

-- Полученное сравнение из system.query_log:
-- agg: query_duration_ms = 23, read_rows = 104, read_bytes = 9984
-- raw: query_duration_ms = 34, read_rows = 268119, read_bytes = 4559263
--
-- В этом запуске agg быстрее и читает намного меньше строк, потому что данные
-- уже предагрегированы по (event_date, platform).
-- Проверка расхождений raw vs agg для этого запроса также дала empty result.
--
-- Ответ на вопрос:
-- raw может быть быстрее agg, если raw-запрос читает очень маленький диапазон
-- данных и хорошо отсекается по партициям/ключу. Еще raw может выиграть, если
-- в aggregate-таблице много маленьких несмердженных частей или запрос плохо
-- совпадает с измерениями, по которым была сделана предагрегация.

/* ============================================================================
База данных: student27

Таблицы:
student27.push_events_raw
student27.push_events_agg
student27.push_counters
student27.push_events_dedup

Распределение raw:
ch-vm-bolwad-sh1-r2-qacbexxah: 337504
ch-vm-bolwad-sh2-r1-qacbexxah: 329002

Репликация:
is_readonly = 0
absolute_delay = 0

Dedup:
count() = 1026546
count() FINAL = 1025474

Performance:
agg: 23 ms, read_rows = 104, read_bytes = 9984
raw: 34 ms, read_rows = 268119, read_bytes = 4559263
============================================================================ */
