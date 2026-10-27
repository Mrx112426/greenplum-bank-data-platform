"""Render a Russian technical report from measured evidence; no database writes."""
import csv
import importlib.util
import json
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parents[2]
E = ROOT/'docs/stage5-evidence'
SPEC = importlib.util.spec_from_file_location('stage5_report_helpers', ROOT/'scripts/sharding/benchmark.py')
B = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(B)


def read(name):
    return json.loads((E/name).read_text())


def table(headers, rows):
    return '\n'.join(['| '+' | '.join(headers)+' |', '| '+' | '.join(['---']*len(headers))+' |'] +
                     ['| '+' | '.join(str(x) for x in row)+' |' for row in rows])+'\n'


def fmt(value):
    return f'{value:.3f}'


def main():
    records, layouts, snapshot = read('measurements.json'), read('layouts.json'), read('snapshot.json')
    assert read('completion.json')['complete']
    if (E/'run-status.json').exists():
        assert read('run-status.json')['state'] in ('complete','verified'), 'Incomplete latest benchmark'
    plan_files=list((E/'plans').glob('*.json'))
    plan_size_mib=sum(p.stat().st_size for p in plan_files)/1024**2
    largest_plan_kib=max(p.stat().st_size for p in plan_files)/1024
    bykey = {(r['group'],r['variant'],r['query']):r for r in records}
    motion_rows, selected = [], []
    source_counts={'tf':snapshot['tables']['ticket_flights']['fingerprint']['rows'],
                   'tickets':snapshot['tables']['tickets']['fingerprint']['rows'],
                   'airports':snapshot['tables']['airports']['fingerprint']['rows'],
                   'rates':snapshot['tables']['exchange_rates']['fingerprint']['rows'],
                   'daily':snapshot['tables']['flight_airport_daily']['fingerprint']['rows'],
                   'boarding_passes':snapshot['tables']['boarding_passes']['fingerprint']['rows'],
                   'flights':snapshot['tables']['flights']['fingerprint']['rows'],
                   'bookings':snapshot['tables']['bookings']['fingerprint']['rows'],
                   'fact':snapshot['details_fingerprint']['rows']}
    for r in records:
        plan=read(f"plans/{r['group']}__{r['variant']}__{r['query']}.json")[0]
        join_motions=[]
        for n in B.walk(plan['Plan']):
            if 'Join' in n['Node Type']:
                join_motions.extend(m['Node Type'] for child in n.get('Plans',[]) for m in B.walk(child) if 'Motion' in m['Node Type'])
        r['join_input_motion']=len(join_motions)
        r['motion_counts']=dict(Counter(m['type'] for m in r['motion']))
        for index,n in enumerate(B.walk(plan['Plan'])):
            if 'Motion' not in n['Node Type']: continue
            children=n.get('Plans',[])
            child=children[0] if children else {}
            exact=None
            # Only a complete unfiltered direct scan has an exact logical input
            # count available from the verified source fingerprint.
            if child.get('Node Type')=='Seq Scan' and 'Filter' not in child:
                exact=source_counts.get(child.get('Relation Name'))
            rows=n.get('Actual Rows',0)*n.get('Actual Loops',1)
            width=n.get('Plan Width',0)
            motion_rows.append({'group':r['group'],'variant':r['variant'],'query':r['query'],
                'node':index,'motion':n['Node Type'],'hash_key':n.get('Hash Key',''),
                'rows_reported_winner':rows,'receivers':n.get('Receivers',1),
                'row_output_lower_bound':rows,
                'row_output_upper_bound':rows*n.get('Receivers',1),
                'verified_unfiltered_logical_input_rows':exact,
                'plan_width_bytes':width,
                'logical_payload_estimate_bytes':exact*width if exact is not None else None})
        key=(r['group'],r['variant'],r['query'])
        if key in {('fact','ticket','ticket_join'),('fact','flight','flight_join'),('fact','bad_fare','boarding_join'),('reference','hash','airport_join'),('reference','replicated','airport_join'),('partition','flat','partition_filter'),('partition','monthly','partition_filter'),('partition','monthly','partition_all')}:
            selected.append('\n### '+' / '.join(key)+'\n')
            def render(n, depth=0):
                info={k:v for k,v in n.items() if k in ('Hash Cond','Hash Key','Group Key','Relation Name','Filter','Number of partitions to scan')}
                selected.append('  '*depth+n['Node Type']+f" [actual_rows={n.get('Actual Rows')}, ms={n.get('Actual Total Time')}, width={n.get('Plan Width')}] "+json.dumps(info,ensure_ascii=False))
                for c in n.get('Plans',[]): render(c,depth+1)
            render(plan['Plan'])
    with (E/'motion-volume.csv').open('w',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(motion_rows[0]),lineterminator='\n')
        writer.writeheader();writer.writerows(motion_rows)
    (E/'selected-plans.txt').write_text('\n'.join(selected).strip()+'\n')
    # Keep the compact derived analysis separate from untouched raw plans.
    (E/'analysis.json').write_text(json.dumps(records,ensure_ascii=False,indent=2)+'\n')
    ready=(E/'verification.json').exists() and read('verification.json').get('passed') and all((E/n).exists() and (E/n).read_text().strip().endswith('OK') for n in ('tests-stage5.txt','tests-stage4.txt','tests-stage3.txt'))
    text=[]
    def add(s): text.append(s.strip()+'\n')
    add('''# Лабораторная №5. Распределение данных и партиционирование Greenplum

Дата реальных экспериментов: 27.10.2026. Ветка `lab5-sharding` основана на коммите лабораторной №4 `4564f5f`. На этапе реализации commit и push не выполнялись; публикация выполняется отдельно после ревью. Merge и изменение PR лабораторной №4 не требуются.

## 1. Цель и границы

Сравнить hash/random/replicated, выбор ключей, data skew, локальность JOIN и Motion на реальных данных авиаперевозок. Отдельно проверить partition pruning. Все DDL, COPY FROM, INSERT, ANALYZE и DROP относятся только к `sharding_stage5_test`. Рабочая `dwh` открывается с принудительным `default_transaction_read_only=on`; исходные ETL, DQ, модель и политики не изменены. Исключение административного соединения `postgres` — единственное создание фиксированной тестовой базы, если её ещё нет.

Зеркалирование и распределение решают разные задачи: два primary исполняют запросы, два mirror синхронно сохраняют их WAL. Mirror не удваивает вычислительное число сегментов. REPLICATED создаёт копии логической таблицы на primary; это не замена mirror.

## 2. Стенд и условия

macOS 26.3.1, Apple Silicon, 16 ГиБ RAM; Docker Engine 27.3.1, Linux ARM64, около 7,65 ГиБ RAM VM. Greenplum `7.0.0 build dev`, образ `andruche/greenplum:7`. Использованы существующие контейнеры; новая инфраструктура и изменения Compose не требуются. Airflow 2.11.2 предоставляет Python/psycopg2 и соединение Greenplum.

До старта в Docker было около 32 ГиБ свободно. Runner проверяет запас не меньше max(6 ГиБ, шестикратный размер dwh), RAM VM не меньше 6 ГиБ, отсутствие running/queued DAG, две пары `u/s` в предпочтительных ролях. Он сериализует запуск через `flock`. Одновременно существует один вариант большой таблицы, кроме общего неизменяемого снимка. После benchmark рабочие копии удалены, базовый снимок сохранён.

Для всех вариантов: GPORCA on, `statement_mem=64MB`, одинаковые столбцы, heap для малых таблиц, AOCO/zstd level 1 для больших. Время CREATE/INSERT сохранено отдельно; ANALYZE и проверка отпечатка исключены из времени запросов. Источники: [environment.json](stage5-evidence/environment.json), [resources.txt](stage5-evidence/resources.txt), [inventory.json](stage5-evidence/inventory.json).

## 3. Согласованный снимок и целостность

Все исходные COPY TO и контрольные запросы выполнены в одной транзакции `REPEATABLE READ READ ONLY` dwh. Загрузка тестового снимка выполняется одной транзакцией; при ошибке она откатывается целиком. Каждый временный COPY-файл закрывается и удаляется автоматически; дампы и строки исходных данных не входят в доказательства.

Скопированы только необходимые столбцы, без имён пассажиров и контактов. Для каждой проекции сравниваются COUNT и две суммы 64-битных половин MD5 текстового ROW, вычисленные NUMERIC без переполнения. Отпечаток не зависит от порядка и сегмента. Это контрольный агрегированный отпечаток, а не криптографическое доказательство полного равенства всех возможных наборов. Все фактические значения и идентификаторы исходных пакетов: [snapshot.json](stage5-evidence/snapshot.json).
''')
    add(table(['Таблица источника','Строк в снимке'],[(v['source'],v['fingerprint']['rows']) for v in snapshot['tables'].values()]))
    add('''Набор для опыта с партициями выведен из снимка ticket_flights JOIN flights и независимо сверен с проекцией `dm.ticket_flight_details`: 8 391 852 строки, те же COUNT и обе контрольные суммы. Дата — scheduled_departure в UTC. Повтор prepare не создаёт копий: проверяет сохранённый снимок и сообщает snapshot_reused. Снимок закреплён: новые публикации dwh не обновляют его автоматически. Все варианты используют одно историческое состояние; текущее состояние dwh фиксируется отдельно перед benchmark и проверяется после, чтобы обнаружить параллельный ETL.

```mermaid
flowchart LR
    D[dwh: READ ONLY] -->|REPEATABLE READ / COPY| B[sharding_stage5_test: stage5_base]
    B --> V[stage5_work: один вариант]
    V --> S[Skew / EXPLAIN ANALYZE / пять повторов]
    S --> E[JSON-планы / CSV / отчёт]
```

Обе базы находятся в существующем кластере с двумя primary и двумя mirror; изоляция базы не означает независимых CPU/I/O.

## 4. Ограничения конкретной GP7

Пробные DDL выполнены в тестовой базе в отдельных транзакциях с ROLLBACK, исходные объекты не менялись. Результаты: [capabilities.json](stage5-evidence/capabilities.json).
''')
    if (E/'capabilities.json').exists():
        add(table(['Проверка','Принято сервером','SQLSTATE / сообщение'],[(r['probe'],r['accepted'],r.get('sqlstate','')+' '+r.get('error','')) for r in read('capabilities.json')]))
    add('''В этой сборке replicated AOCO **поддерживается**, хотя в основных опытах справочники оставлены heap для одинаковых условий. PRIMARY KEY должен быть совместим с ключом распределения; составные ключи в эксперименте не вводят искусственного UNIQUE, которого нет в исходной модели. Переход с ticket_no на `(ticket_no,flight_id)` меняет hash: он не сохраняет колокацию с таблицей, распределённой только по ticket_no.

## 5. Data skew

Для каждого primary учтены строки, включая нулевые. Метрики: min, max, mean, max/mean; CV_population = population_stddev/mean; отдельно сохранён CV_sample_pct = sample_stddev/mean × 100 для сопоставления с gp_toolkit. Для пустой таблицы отношение и CV — null, деления на ноль нет.

У replicated-таблиц GP7 отсутствует gp_segment_id. Физические копии посчитаны через `gp_dist_random()` с `gp_execution_segment()`; логический fingerprint считает строки один раз. Нельзя интерпретировать сумму физических replicated-копий как увеличение числа бизнес-строк. Методика подсчёта и нулевых сегментов согласована с [Broadcom](https://knowledge.broadcom.com/external/article/295523/how-is-the-value-skccoeff-under-gptoolki.html).
''')
    add(table(['Группа','Вариант','Content 0','Content 1','max/avg','CV population','CREATE+INSERT, мс'],[(r['group'],r['variant'],r['counts']['0'],r['counts']['1'],f"{r['max_avg']:.4f}",f"{r['cv_population']:.4f}",fmt(r['load_ms'])) for r in layouts]))
    add('''По fare_conditions на одном primary 8 251 887 строк, на другом 139 965: ключ имеет всего три значения и создаёт выраженный перекос. Контрольный пример из 1000 строк с одинаковым ключом дал 0/1000, max/avg=2 и CV_population=1: [empty-segment.json](stage5-evidence/empty-segment.json). Для composite равномерность почти идеальна, но она не гарантирует локальности соединения.

Полные показатели: [layouts.csv](stage5-evidence/layouts.csv), [layouts.json](stage5-evidence/layouts.json). Физическое число строк проверено против логического fingerprint; для replicated каждая копия равна логическому числу строк.

## 6. Запросы и методика времени

Реальные SQL находятся в [004_workload.sql](../sql/sharding/004_workload.sql): соединения билетов/перелётов/посадок, рейсы по аэропортам, продажи по датам бронирования, фильтры по билету/рейсу, справочник аэропортов, оконный расчёт курсов и дневная агрегация. Сумма бронирований считается после агрегации билетов по book_ref, чтобы не умножать её при JOIN.

Фильтры выбраны из существующих ключей и сохранены в [filters.json](stage5-evidence/filters.json). Перед измерением выполняются запрос проверки эквивалентности с SHA-256 упорядоченного результата и один отдельный прогрев. Затем пять EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON), без параллельного benchmark. Hash/random/replicated-варианты идут в воспроизводимом перемешанном порядке; порядок запросов внутри каждого повтора тоже перемешан. В опыте с партициями порядок фиксирован: flat, затем monthly. Варианты выполняются последовательно, а не чередуются в отдельных раундах: это ограничение экономии места и возможный источник временного дрейфа.

В таблицах — серверный Execution Time, медиана [min–max], миллисекунды. Client wall time и стандартное отклонение также сохранены. Времена дочерних узлов не суммируются: они могут перекрываться и относиться к разным выбранным workers. Это инструментированные тёплые запросы; cache flush/перезапуск VM не выполнялись. Оценки cost оптимизатора не являются миллисекундами. Пять повторов не дают основания объявлять статистическую значимость по одной медиане.
''')
    group_names={'fact':'Большая ticket_flights: пять стратегий','tickets':'Ключ tickets: билет или бронирование','reference':'Маленький справочник airports','rates':'Курсы: баланс против локальности окна','daily':'Дневная витрина по аэропортам','partition':'Независимый опыт с партициями'}
    for group,title in group_names.items():
        add('### '+title)
        rows=[r for r in records if r['group']==group]
        names=list(dict.fromkeys(r['query'] for r in rows))
        variants=list(dict.fromkeys(r['variant'] for r in rows))
        add(table(['Вариант']+names, [[v]+[f"{bykey[group,v,q]['median_ms']:.3f} [{bykey[group,v,q]['min_ms']:.3f}–{bykey[group,v,q]['max_ms']:.3f}]" for q in names] for v in variants]))
    add(f'''Полные измерения: [measurements.csv](stage5-evidence/measurements.csv), [measurements.json](stage5-evidence/measurements.json). Для каждого сочетания group/variant/query сохранены пять полных серверных JSON-планов в `docs/stage5-evidence/plans/`: всего 46 сочетаний и 230 планов. Они сгруппированы в 46 файлов общим размером {plan_size_mib:.2f} МиБ (максимальный файл {largest_plan_kib:.1f} КиБ); сохранены все повторы, чтобы независимо проверить времена и неизменность структуры планов. JSON записан компактно, по одному полному плану на строку; поля и значения не удалены. Для чтения человеком выделены восемь репрезентативных деревьев в selected-plans.txt. Все 46 сочетаний прошли проверку эквивалентности. Ошибок выполнения аналитических SQL в измеренной серии не было.

## 7. Motion, локальность и объёмы

Gather объединяет потоки на координаторе, Redistribute пересылает по hash-ключу, Broadcast размножает поток на получателей. Наличие Gather для выдачи результата не означает неудачное распределение. В таблице R/B/G — количество узлов Redistribute/Broadcast/Gather; последний столбец — Motion внутри входных ветвей JOIN, без агрегирования над JOIN.
''')
    add(table(['Группа','Вариант','Запрос','R/B/G','Motion под JOIN'],[(r['group'],r['variant'],r['query'],'/'.join(str(r['motion_counts'].get(k+' Motion',0)) for k in ('Redistribute','Broadcast','Gather')),r['join_input_motion']) for r in records]))
    add('''Для ticket_flights BY(ticket_no) JOIN tickets по ticket_no локален: Motion требуется последующей группировке по book_ref, а не самому соединению. BY(flight_id) делает локальным JOIN flights, сохраняя Motion группировки по аэропорту. RANDOMLY и composite могут распределять строки равномерно, но требуют подготовки потоков для соединения с существующими таблицами. У маленького airports REPLICATED убирает необходимость Broadcast этой стороны JOIN; времена около 30 мс перекрываются, поэтому ускорение именно этого запроса не доказано.

Объёмы: [motion-volume.csv](stage5-evidence/motion-volume.csv). В GP7 JSON Actual Rows использует статистику выбранного worker, а не автоматически сумму всех workers ([исходный код explain_gp.c](https://github.com/greenplum-db/gpdb-archive/blob/main/src/backend/commands/explain_gp.c)). Сохранены reported rows, число receivers и границы общего output [reported; reported × receivers] для наблюдавшихся однопроходных узлов. Когда непосредственный вход Motion — полный Seq Scan без фильтра, точное логическое число входных строк взято из проверенного fingerprint; оценка логической полезной нагрузки на входе — это число × Plan Width; для Broadcast выход дополнительно содержит копии на получателях. Например, дополнительное перераспределение полного ticket_flights затрагивает 8 391 852 логических строк. В flight_join у bad_fare ширина этого потока 10 байт: оценка логического payload 83 918 520 байт (около 80 МиБ).

Это анализ объёма строк и оценка payload, **не измеренные байты сетевых пакетов**. Plan Width — оценка, заголовки/сжатие и локальные пересылки внутри VM не измерены. Вывод о сетевой пропускной способности физического кластера не делается. Краткие деревья, полученные из серверных JSON, доступны в [selected-plans.txt](stage5-evidence/selected-plans.txt); исходные планы сохранены отдельно.

## 8. Партиционирование по дате

Обе таблицы имеют одинаковые пять столбцов, все 8 391 852 строки, AOCO/zstd, BY(ticket_no), один и тот же fingerprint и одинаковое число строк на каждом primary. Единственный меняемый фактор — flat против RANGE(flight_date) EVERY INTERVAL '1 month'. Создано 14 месячных партиций от октября 2015 до ноября 2016 включительно. Для каждой из двух конфигураций выполнены оба запроса с пятью повторами.

```sql
SELECT fare_conditions,count(*),sum(amount)
FROM stage5_work.fact
WHERE flight_date >= DATE '2016-10-01' AND flight_date < DATE '2016-10-08'
GROUP BY fare_conditions ORDER BY fare_conditions;
```

В месячном плане Dynamic Seq Scan показывает `Number of partitions to scan: 1` для фильтра и 14 без фильтра. Это наблюдаемое pruning, а не предположение по имени таблицы: [partition_monthly.json](stage5-evidence/partition_monthly.json) и планы partition__monthly__*.json.
''')
    flat=bykey['partition','flat','partition_filter']; month=bykey['partition','monthly','partition_filter']
    add(f"Для недельного фильтра медиана {flat['median_ms']:.3f} → {month['median_ms']:.3f} мс, отношение {flat['median_ms']/month['median_ms']:.2f}×. Диапазоны повторов не пересеклись. Без фильтра результаты flat/monthly перекрываются; ускорение полного сканирования не подтверждено. Этот результат не смешивается со сменой ключа распределения.")
    add('''## 9. Рекомендации для Core и DM

Рекомендации относятся к измеренной нагрузке и размеру данных, а не ко всем возможным запросам. Автоматические миграции dwh отсутствуют.
''')
    add(table(['Таблицы','Рекомендация','Основание'],[
      ('core.aircrafts / airports / seats','Сохранить REPLICATED','Малые справочники; локальные JOIN. Для airports выигрыш времени над hash/random не доказан.'),
      ('core.bookings','Сохранить BY(book_ref)','Высокая кардинальность; естественное соединение с агрегацией билетов по бронированию.'),
      ('core.flights','Сохранить BY(flight_id)','Равномерный уникальный ключ; вариант фактов BY(flight_id) подтверждает локальность соответствующего JOIN.'),
      ('core.tickets / ticket_flights / boarding_passes','Сохранить BY(ticket_no)','Колокация существующих соединений, хороший баланс. Альтернативы не показали универсального устойчивого выигрыша по нагрузке.'),
      ('core.exchange_rates','Сохранить BY(currency_code)','Только 90 строк; окно по валюте локально, currency и replicated имеют пересекающиеся интервалы времени.'),
      ('dm.ticket_flight_details','Сохранить BY(ticket_no); рассмотреть месячные партиции даты','Политика согласована с основными фактами; отдельный опыт с эквивалентной проекцией доказал пользу pruning для дат.'),
      ('dm.flight_airport_daily','Кандидат BY(flight_date) для измеренного GROUP BY flight_date','В измеренном GROUP BY date уменьшены skew и Motion; требует отдельной явной миграции и проверки реальной смеси запросов.'),
      ('dm.booking_sales_daily','Сохранить BY(booking_date)','392 строки, естественная дата зерна. Отдельного сравнения альтернатив этой малой витрины не проводилось.'),
      ('dm.exchange_rates_daily','Сохранить BY(currency_code)','Вывод опыта с тем же малым набором и оконным доступом; разница с REPLICATED мала и перекрывается.')]))
    daily0=bykey['daily','airport','daily_group']; daily1=bykey['daily','date','daily_group']
    add(f"Для дневной витрины в этой серии BY(date): {daily1['median_ms']:.3f} мс против {daily0['median_ms']:.3f} мс у BY(airport); максимум date {daily1['max_ms']:.3f} меньше минимума airport {daily0['min_ms']:.3f}. Это основание предложить date для данного запроса, но не автоматически переписывать рабочую модель. Более равномерный composite сам по себе не дал лучшего результата, чем date.")
    add('''Для больших JOIN интервалы многих вариантов перекрываются: небольшое отличие медиан не объявляется доказанным ускорением. У bad_fare фильтры значительно медленнее и есть непосредственно измеренный skew, поэтому этот вариант исключается. Составной ключ не предлагается просто ради меньшего CV. Настройка распределения не устраняет стоимость больших HashAggregate, сортировок и JOIN.

## 10. Тесты и совместимость этапов 1–4

`tests/test_sharding.py` разделяет семь unit-тестов и пять live-тестов: защита имени/фактической базы и read-only dwh, формулы skew с нулями, контроль изменения результатов, полная матрица, пять планов каждого запроса, физические replicated-копии, целостность снимка, реальные зеркала и pruning 1/14. Live включаются явно через STAGE5_LIVE=1.
''')
    for filename,label in [('tests-stage5.txt','Лабораторная 5'),('tests-stage4.txt','Лабораторная 4'),('tests-stage3.txt','Лабораторная 3')]:
        if (E/filename).exists():
            content=(E/filename).read_text()
            summary=next((line for line in content.splitlines() if line.startswith('Ran ')), 'Нет сводки')
            tail=summary+'; '+content.strip().splitlines()[-1]
            add(f"{label}: [{filename}](stage5-evidence/{filename}); итог: `{tail}`.")
    add('''Проверка зеркал, исходный контрольный SQL, сервисы и HTTP: [mirrors-final.txt](stage5-evidence/mirrors-final.txt), [edw-verify.txt](stage5-evidence/edw-verify.txt), [services.txt](stage5-evidence/services.txt), [airflow-health.json](stage5-evidence/airflow-health.json), [minio-health.txt](stage5-evidence/minio-health.txt). Состояние опубликованных пакетов и контрольные количества/суммы dwh сравниваются с состоянием перед измерением; обе пары остались u/s с исходными ролями. Независимый [gpstate](stage5-evidence/gpstate-final.txt) подтвердил нулевой остаток отправки/flush/replay WAL; [размеры баз и свободный диск](stage5-evidence/disk-final.txt) сохранены отдельно. DQ и ETL не перезапускались для изменения рабочих данных; тесты EDW используют только edw_stage3_test. Проверка повторного prepare: [snapshot-reuse.json](stage5-evidence/snapshot-reuse.json).

Во время разработки была ошибка shell-обёртки после уже завершённых измерений: файл редактировался во время ожидания Python. Все 46 сравнений и completion.json к тому моменту были сохранены. Окончательная обёртка отдельно проверена командами capabilities/verify/status/prepare; диагностическая ошибка не выдаётся за сбой SQL или потерю данных. Работающая реализация этапов 1–4 не переписывалась.

## 11. Воспроизведение

Из корня репозитория при работающей исходной платформе и опубликованных данных dwh:

```bash
bash scripts/sharding/run.sh status
bash scripts/sharding/run.sh all
STAGE5_LIVE=1 python3 tests/test_sharding.py
python3 scripts/sharding/report.py
```

Отдельные шаги: `prepare`, `capabilities`, `benchmark`, `verify`. Benchmark занимает несколько минут и исполняется последовательно. `prepare` повторно проверяет сохранённый снимок, не импортирует его ещё раз. Набор 2016 года и дата фильтра намеренно фиксированы. В Docker не должно быть одновременного ETL; runner не меняет расписания и останавливается при активном DAG.

Python подключается только к фиксированной sharding_stage5_test для записи и проверяет фактическое имя соединения. `stage5_base` — неизменяемая проекция снимка, `stage5_work` — явно принадлежащая этому эксперименту временная схема; её объекты пересоздаются на каждом варианте. Чужая существующая тестовая база без маркера принадлежности отвергается. Исходные Docker-тома и базы не удаляются.

После успешного benchmark raw evidence сначала находится в `/tmp/stage5-evidence` контейнера Airflow, затем копируется в docs/stage5-evidence. В случае ошибки частичные доказательства тоже копируются; run-status=running блокирует выдачу старого completion за новый успешный запуск. В Git не должны попадать .cache, .env, COPY-файлы, дампы или рабочие airflow/logs. После пересоздания контейнера Airflow его /tmp может исчезнуть: host-доказательства сохраняются, а runtime verify потребует повторного benchmark на сохранённом снимке.

## 12. Ограничения и вывод

Стенд содержит два primary на одной VM; это не измерение масштабирования по нескольким физическим серверам. Общие CPU, page cache и I/O, фоновые процессы Docker/Airflow и последовательный порядок вариантов влияют на время. Измерены проекции столбцов и конкретные SELECT, а не скорость всего ETL, не все запросы пользователей и не wire traffic. Нет универсального победителя hash/random/replicated.

Подтверждены реальное распределение строк, намеренный skew, корректный учёт пустых сегментов и replicated-копий, изменение Motion и одинаковые результаты всех запросов. Главный вывод для существующей платформы — сохранить колокацию крупных фактов по ticket_no, не менять ключ ради одного CV; для дневной витрины предложен date, а отдельный опыт показал пользу месячного pruning при фильтре даты. Рабочая dwh и репликация лабораторной №4 сохранены. В приложенных доказательствах completion и verification успешны, все live-тесты пройдены. Лабораторная №5 выполнена.
''')
    document='\n'.join(text)
    if not ready:
        document=document.replace('В приложенных доказательствах completion и verification успешны, все live-тесты пройдены. Лабораторная №5 выполнена.', 'Проверки ещё не подтверждены полностью; не считать лабораторную завершённой до успешных verification и live-тестов.')
    (ROOT/'docs/stage5-verification.md').write_text(document)
    print('Wrote docs/stage5-verification.md, motion-volume.csv, selected-plans.txt, analysis.json')


if __name__=='__main__': main()
