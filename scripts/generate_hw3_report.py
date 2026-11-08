#!/usr/bin/env python3
"""Regenerate the editable report from saved evidence; never contacts services.

Dependencies: report/report_requirements.txt, installed in the existing .venv.
Run: .venv/bin/python scripts/generate_hw3_report.py
Six explicitly separated pages are designed; rendered pagination needs Word/Writer.
"""

import csv
import hashlib
import json
from pathlib import Path
import re
import statistics
import zipfile
from xml.etree import ElementTree as ET

from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT, WD_CELL_VERTICAL_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Inches, Pt, RGBColor
from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / 'report'
SOURCES = []


def read(path):
    SOURCES.append(path)
    return (ROOT / path).read_text(encoding='utf-8')


def load(path):
    return json.loads(read(path))


def source_line(text):
    p = doc.add_paragraph('Evidence: ' + text)
    p.paragraph_format.space_after = Pt(4)
    for run in p.runs:
        run.font.size = Pt(8)
        run.font.color.rgb = RGBColor.from_string('526171')


def paragraph(text, style=None):
    return doc.add_paragraph(text, style)


def heading(text, level=2):
    doc.add_heading(text, level=level)


def caption(text):
    doc.add_paragraph(text, 'Caption')


def table(headers, rows, widths, font_size=9):
    t = doc.add_table(rows=1, cols=len(headers))
    t.style = 'Table Grid'
    t.autofit = False
    t.alignment = WD_TABLE_ALIGNMENT.CENTER
    for cell, label in zip(t.rows[0].cells, headers):
        cell.text = label
    for row in rows:
        for cell, value in zip(t.add_row().cells, row):
            cell.text = str(value)
    for row_index, row in enumerate(t.rows):
        trpr = row._tr.get_or_add_trPr()
        trpr.append(OxmlElement('w:cantSplit'))
        if row_index == 0:
            trpr.append(OxmlElement('w:tblHeader'))
        for cell, width in zip(row.cells, widths):
            cell.width = Inches(width)
            cell.vertical_alignment = WD_CELL_VERTICAL_ALIGNMENT.CENTER
            if row_index == 0:
                shading = OxmlElement('w:shd')
                shading.set(qn('w:fill'), '163A59')
                cell._tc.get_or_add_tcPr().append(shading)
            for p in cell.paragraphs:
                p.paragraph_format.space_after = Pt(3)
                p.paragraph_format.space_before = Pt(3)
                p.paragraph_format.line_spacing = 1.0
                for r in p.runs:
                    r.font.size = Pt(font_size)
                    if row_index == 0:
                        r.bold = True
                        r.font.color.rgb = RGBColor(255, 255, 255)
    for column, width in zip(t.columns, widths):
        column.width = Inches(width)
    return t


def code(text):
    p = doc.add_paragraph()
    p.paragraph_format.space_after = Pt(5)
    p.paragraph_format.line_spacing = 1.0
    r = p.add_run(text)
    r.font.name = 'Liberation Mono'
    r.font.size = Pt(8.5)


def question(number, answer):
    p = doc.add_paragraph(style='Heading 2')
    p.paragraph_format.space_before = Pt(7)
    p.paragraph_format.space_after = Pt(3)
    p.add_run(questions[number])
    paragraph(answer)
    answers[number] = answer


def architecture():
    im = Image.new('RGB', (1700, 390), 'white')
    draw = ImageDraw.Draw(im)
    fontfile = '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf'
    font = ImageFont.truetype(fontfile, 27)
    small = ImageFont.truetype(fontfile, 22)
    positions = [30, 370, 710, 1050, 1390]
    names = ['MovieLens CSV', 'HDFS raw', 'Spark local', 'MongoDB', 'PrestoDB']
    subtitles = ['original files', 'replication 1', 'batch export', 'movie_stats', 'SQL + connector']
    for x, name, sub in zip(positions, names, subtitles):
        draw.rounded_rectangle((x, 25, x+280, 135), radius=12, fill='#eaf1f7', outline='#163a59', width=3)
        draw.text((x+140, 65), name, fill='#163a59', font=font, anchor='mm')
        draw.text((x+140, 103), sub, fill='#334455', font=small, anchor='mm')
    for x in positions[:-1]:
        draw.line((x+283,80,x+332,80),fill='#163a59',width=4)
        draw.polygon([(x+332,80),(x+318,72),(x+318,88)],fill='#163a59')
    draw.text((1020,165), 'JSON Lines transfer', font=small, anchor='mm', fill='#334455')
    draw.text((1540,165), 'read via connector', font=small, anchor='mm', fill='#334455')
    draw.line((160,140,160,245,350,245),fill='#526171',width=3)
    draw.polygon([(350,245),(336,237),(336,253)],fill='#526171')
    draw.rounded_rectangle((355,200,970,300),radius=12,fill='#f1f4ed',outline='#526171',width=3)
    draw.text((662,234),'Python producer → Kafka (E1)',font=font,anchor='mm',fill='#163a59')
    draw.text((662,273),'one KRaft broker; 3 partitions; RF=1',font=small,anchor='mm',fill='#334455')
    draw.text((1130,245),'Independent replay branch',font=small,fill='#334455')
    draw.text((1130,280),'No Kafka-to-Spark streaming',font=small,fill='#334455')
    path = REPORT / 'assets/architecture.png'
    path.parent.mkdir(parents=True, exist_ok=True)
    im.save(path)
    return path


# Read the original questions verbatim, not a reconstructed question template.
with zipfile.ZipFile(ROOT / 'HW3_BDSP_2026.docx') as archive:
    xml = ET.fromstring(archive.read('word/document.xml'))
original = [''.join(p.itertext()) for p in xml.findall('.//{http://schemas.openxmlformats.org/wordprocessingml/2006/main}p')]
questions = {int(p[1]): p for p in original if re.match(r'^Q[1-8]\. ', p)}
assert set(questions) == set(range(1, 9))
SOURCES.append('HW3_BDSP_2026.docx')
env = load('report/environment_snapshot.json')
assert read('report/component_snapshot.txt').startswith('Hadoop 3.3.5\n')
k5 = load('logs/e1/ratings-5k.json')
k30 = load('logs/e1/ratings-30k.json')
for value in (k5, k30):
    txt = read(f"logs/e1/{value['topic']}.txt")
    assert f"Elapsed seconds: {value['elapsed_seconds']:.6f}" in txt
    assert value['successful_records'] == value['requested_records'] and value['failed_records'] == 0
offsets = read('logs/e1/kafka-offsets.txt')
offset_totals = {topic: sum(int(v) for v in re.findall(re.escape(topic) + r':\d+:(\d+)', offsets))
                 for topic in ('ratings-5k', 'ratings-30k')}
assert offset_totals == {'ratings-5k': 10000, 'ratings-30k': 60000}
spark = load('report/spark/benchmark_summary.json')
trials = list(csv.DictReader(read('report/spark/benchmark_results.csv').splitlines()))
for fmt in ('csv', 'parquet'):
    times = [float(r['elapsed_seconds']) for r in trials if r['format'] == fmt and r['phase'] == 'timed']
    assert len(times) == 3
    assert times == spark['statistics'][fmt]['runtimes_seconds']
    assert statistics.mean(times) == spark['statistics'][fmt]['mean_seconds']
    assert statistics.median(times) == spark['statistics'][fmt]['median_seconds']
mongo = load('report/mongodb/benchmark_results.json')
for when in ('before', 'after'):
    exp = load(f'report/mongodb/explain_{when}_index.json')
    for field in ('nReturned', 'totalDocsExamined', 'executionTimeMillis'):
        assert exp['executionStats'][field] == mongo[f'{when}_index'][field]
presto = load('report/presto/verification_results.json')
assert presto['top10'] == presto['mongo_top10'] and presto['all_four_fields_match_all_ten']
assert presto['warnings'] == presto['errors'] == []
raw_listing = read('logs/hdfs/raw-files.txt')
metadata = read('logs/hdfs/file-metadata.txt')
fsck = read('logs/hdfs/fsck-raw.txt')
for filename, size in [('movies.csv',494431),('ratings.csv',2483723)]:
    assert filename in raw_listing
    assert f'{filename} | size={size} bytes' in metadata
    assert f'{filename} {size} bytes, replicated: replication=1, 1 block(s):  OK' in fsck
    with zipfile.ZipFile(ROOT / 'data/ml-latest-small.zip') as sourcezip:
        assert sourcezip.read('ml-latest-small/' + filename) == (ROOT / 'data/ml-latest-small' / filename).read_bytes()
with (ROOT / 'data/ml-latest-small/ratings.csv').open() as f:
    ratings = list(csv.DictReader(f))
with (ROOT / 'data/ml-latest-small/movies.csv').open() as f:
    movies = list(csv.DictReader(f))
assert len(ratings) == 100836 and len(movies) == 9742
assert len({r['userId'] for r in ratings}) == 610
for path in ['kafka/producer.py', 'kafka/docker-compose.yml', 'spark/run_hw3.py',
             'mongodb/import_and_benchmark.py', 'presto/verify_hw3.py', 'presto/compose.yaml',
             'logs/hdfs/jps.txt', 'logs/hdfs/dfsadmin-report.txt',
             'logs/spark/' + spark['run_id'] + '.log',
             'logs/mongodb/' + mongo['run_id'] + '.log',
             'logs/presto/' + presto['run_id'] + '.log']:
    read(path)
reduction = 100 * (mongo['before_index']['totalDocsExamined'] - mongo['after_index']['totalDocsExamined']) / mongo['before_index']['totalDocsExamined']

doc = Document()
sec = doc.sections[0]
sec.page_width, sec.page_height = Inches(8.27), Inches(11.69)
sec.top_margin = sec.bottom_margin = Inches(.65)
sec.left_margin = sec.right_margin = Inches(.7)
sec.header_distance = sec.footer_distance = Inches(.28)
normal = doc.styles['Normal']
normal.font.name = 'Liberation Sans'
normal.font.size = Pt(10.5)
normal.paragraph_format.line_spacing = 1.04
normal.paragraph_format.space_after = Pt(5)
for name, size in [('Heading 1',16),('Heading 2',11),('Title',21),('Caption',8.5)]:
    style = doc.styles[name]
    style.font.name = 'Liberation Sans'
    style.font.size = Pt(size)
    style.font.color.rgb = RGBColor.from_string('163A59')
    style.paragraph_format.space_before = Pt(8)
    style.paragraph_format.space_after = Pt(5)
    if name == 'Heading 2':
        style.font.bold = True
header = sec.header.paragraphs[0]
header.text = 'BDSP 2026  |  Homework 3'
header.style = 'Caption'
footer = sec.footer.paragraphs[0]
footer.alignment = WD_ALIGN_PARAGRAPH.RIGHT
footer.add_run('Page ')
field = OxmlElement('w:fldSimple')
field.set(qn('w:instr'), 'PAGE')
footer._p.append(field)
doc.core_properties.title = 'BDSP HW3 — A Small Data Pipeline'
doc.core_properties.subject = 'Evidence-based report: HDFS, Kafka, Spark, MongoDB and PrestoDB'
answers = {}

# Page 1
doc.add_heading('BDSP HW3 — A Small Data Pipeline', 0)
heading('Environment and data', 1)
paragraph(f"Host inspection on 8 November 2026: Ubuntu 26.04 LTS (x86_64), kernel {env['kernel']}; AMD Ryzen 5 5500U with Radeon Graphics, six physical cores and 12 hardware threads. Linux exposes {env['mem_total_bytes']:,} bytes ({env['mem_total_bytes']/2**30:.2f} GiB) RAM. This is below the tutorial’s approximately 16 GB recommendation, so the pipeline was developed in phases rather than requiring every service concurrently.")
caption('Table 1. Component versions and evidence basis.')
table(['Component','Version / configuration'],[
    ['HDFS / Hadoop','3.3.5 installation (report-time version check); one DataNode; replication 1'],
    ['Kafka','apache/kafka:3.9.1 configured image; one combined KRaft broker/controller'],
    ['Spark runtime','Python 3.12.16; PySpark 3.5.9; Java 8u504; local[*]'],
    ['Hadoop client in Spark','3.3.4 bundled client, distinct from the 3.3.5 HDFS installation'],
    ['MongoDB / driver','7.0.45 observed; PyMongo 4.19.0'],
    ['PrestoDB','0.298.1-9e1b45f observed (image 0.298.1); image supplies Java 17'],
], [1.5,5.37])
paragraph('The official ml-latest-small archive contains 100,836 ratings from 610 users and 9,742 movie records. Local CSV counts match the Spark input counts; both original CSV byte streams also match the downloaded ZIP. There are 9,724 rated movie groups; filtering to at least 20 ratings produces 1,297 summary rows. The 18 remaining movie records have no ratings in this dataset.')
doc.add_picture(str(architecture()), width=Inches(6.85))
caption('Figure 1. Local batch pipeline and separate Kafka replay branch. Arrows represent reads/transfers; no streaming path or permanent Presto data copy is implied.')
heading('Services active during the recorded phases')
paragraph('E1 necessarily used Kafka and the producer; other services’ historical state was not recorded. E2’s startup log records the NameNode, DataNode and SecondaryNameNode; Spark then ran in that application. No Kafka or MongoDB process appears in that startup inventory, but it is not a complete container/service audit. E3 records a successful MongoDB ping and HDFS transfer; other services’ state was not recorded. Part 5 records both Presto and MongoDB responding; unrelated services’ state was not recorded.')
source_line('report/environment_snapshot.json and component_snapshot.txt; report/spark/benchmark_summary.json; logs/spark/20261107T093200113988276-20197.log; report/mongodb/benchmark_results.json; report/presto/verification_results.json. Kafka version is configuration evidence, not a saved broker-version response.')

# Page 2
doc.add_page_break()
heading('Implementation',1)
heading('HDFS: unchanged raw storage')
paragraph('The single-node HDFS namespace is hdfs://localhost:9000. Both CSV files were uploaded to /user/bdsp/hw3/raw. The saved listing shows movies.csv (482.8 K) and ratings.csv (2.4 M); exact metadata and fsck results are summarized below. FSCK reports HEALTHY, one DataNode, two validated blocks, and no missing or corrupt blocks.')
caption('Table 2. Actual HDFS file evidence; blocks are observed, not inferred.')
table(['File','Logical bytes','Configured block bytes','Replication','fsck blocks'],[
 ['movies.csv','494,431','134,217,728','1','1'],
 ['ratings.csv','2,483,723','134,217,728','1','1'],
], [1.2,1.1,1.9,1.1,1.57])
code('hdfs dfs -ls -h /user/bdsp/hw3/raw\nhdfs fsck /user/bdsp/hw3/raw -files -blocks -locations')
source_line('logs/hdfs/raw-files.txt, file-metadata.txt, fsck-raw.txt and dfsadmin-report.txt.')
heading('Kafka: replay ingestion, not a streaming Spark dependency')
paragraph('kafka/docker-compose.yml defines one KRaft node with broker/controller roles, no ZooKeeper, three default partitions and replication factor 1. Single-broker offsets/transaction topic replication and minimum in-sync replicas are also 1. Host Python connects to localhost:9092; the controller listener is internal on 9093. Each E1 topic is intended to have three partitions and RF=1; offsets confirm partitions 0–2 exist, but a broker topic-description output was not retained to independently establish topic-level replication.')
paragraph('kafka/producer.py reads CSV via DictReader, keys complete JSON records by userId, uses acks="all", and examines delivery futures after flush. time.perf_counter starts immediately before CSV iteration and includes serialization, send calls and flush, excluding producer initialization and close. Successful acknowledgements, failures and unresolved records are reported separately. Both retained runs have zero failed, unresolved or unsubmitted records.')
heading('Spark: typed batch processing and export')
paragraph('spark/run_hw3.py uses one local[*] SparkSession and four shuffle partitions. Explicit ratings types are (userId int, movieId int, rating double, timestamp int); movie fields are (movieId int, title string, genres string). GroupBy/avg/count, title join, rating_count ≥ 20 filtering and four-decimal rounding produce (movie_id, title, rating_count, avg_rating). The most-rated movie is Forrest Gump (1994): 329 ratings, mean 4.1641. All Parquet and JSON summary rows were read back and compared exactly.')
paragraph('Under /user/bdsp/hw3/curated/, outputs are ratings_parquet, movie_stats_parquet and movie_stats_json. Only the small JSON export uses coalesce(1); raw HDFS files are never overwritten. Run with bash scripts/run_spark_hw3.sh; the launcher selects the existing .venv, Java 8 and Hadoop configuration.')
source_line('spark/run_hw3.py; report/spark/benchmark_summary.json; logs/spark/20261107T093200113988276-20197.log.')
heading('MongoDB: query-oriented serving')
paragraph('scripts/run_mongodb_hw3.sh downloads only the existing HDFS JSON export to a fresh local staging directory. mongodb/import_and_benchmark.py validates part-*.json files, ignores metadata, resets only bdsp_hw3.movie_stats, and inserts batches of 500. All 1,297 documents match source values. MongoDB $type verifies movie_id and rating_count are int and avg_rating is double for every document. The sample is Toy Story (1995), movie_id 1, count 215, average 3.9209; the later movie_id_1 index is nonunique.')
source_line('report/mongodb/benchmark_results.json and explain_before_index.json / explain_after_index.json; mongodb/import_and_benchmark.py.')

# Page 3
doc.add_page_break()
heading('Presto: SQL over MongoDB (Implementation continued)')
paragraph(f"The observed PrestoDB build is {presto['presto_version']}; MongoDB is {presto['mongodb_version']}. Presto joins the existing bdsp-hw3-mongodb_default network and uses mongodb.seeds=mongodb:27017. No Hive Metastore, Hive connector, HDFS connector or second MongoDB instance is needed. Presto’s single coordinator also executes query work.")
paragraph('The host HTTP verifier executed SHOW CATALOGS, SHOW SCHEMAS FROM mongodb, SHOW TABLES FROM mongodb.bdsp_hw3, and DESCRIBE mongodb.bdsp_hw3.movie_stats. Saved responses show catalog mongodb, schema bdsp_hw3 and table movie_stats. DESCRIBE reports movie_id bigint, title varchar, rating_count bigint and avg_rating double. Verification used Presto’s HTTP API rather than the tutorial’s CLI interface; the SQL is identical and the bundled CLI commands remain documented.')
code(presto['sql'].strip())
caption('Table 3. Actual Presto top-ten output, in the required deterministic order.')
table(['movie_id','title','rating_count','avg_rating'],[
 [r['movie_id'],r['title'],r['rating_count'],f"{r['avg_rating']:.4f}"] for r in presto['top10']
], [.8,4.05,1.05,.97],9)
paragraph('The verifier compared the equivalently filtered and sorted MongoDB results for all ten records, exceeding the required top-three check. All 40 comparisons (four fields × ten rows) matched, including movie_id, title, rating_count and avg_rating for IDs 1104, 318 and 922 at ranks 1–3. The movie_stats document fingerprint was unchanged; errors were none and warnings were []. The final run exited 0.')
paragraph('The first run’s version check treated the build suffix as a release mismatch. The check was corrected to compare the release component while retaining the full reported build; the successful final run has no version warning. Existing connector _schema metadata was checked before SQL discovery, avoiding implicit metadata creation during verification. Presto’s SQL access control is read-only.')
code('bash scripts/run_presto_hw3.sh --server http://localhost:8080')
source_line('presto/sql/top10.sql; presto/etc/catalog/mongodb.properties; report/presto/verification_results.json; report/presto/runs/' + presto['run_id'] + '/; logs/presto/' + presto['run_id'] + '.log. Complete field-level comparisons are retained in JSON.')

# Page 4
doc.add_page_break()
heading('Experiments',1)
paragraph('Table 4 consolidates E1–E3. Timings are saved observations, not new runs performed for this report. Warm-ups are excluded from E2 statistics; E3 timings are server executionTimeMillis, not client wall-clock time.')
experiment_rows = []
for k in [k5,k30]:
    experiment_rows.append(['E1', f"{k['topic']}\nN={k['requested_records']:,}",
       f"{k['elapsed_seconds']:.6f} s; {k['throughput_records_per_second']:,.2f} records/s\nSuccessful {k['successful_records']:,}; failed 0",
       f"Final offsets total {offset_totals[k['topic']]:,}; two topic replays reported. Per-run success differs from cumulative topic volume."])
for fmt in ['csv','parquet']:
    s = spark['statistics'][fmt]
    warm = next(float(r['elapsed_seconds']) for r in trials if r['format']==fmt and r['phase']=='warmup')
    experiment_rows.append(['E2',fmt.upper(),
        f"Warm-up {warm:.6f} s\nTrials: " + ', '.join(f'{v:.6f}' for v in s['runtimes_seconds']) +
        f" s\nMean {s['mean_seconds']:.6f}; median {s['median_seconds']:.6f} s\nRange {s['min_seconds']:.6f}–{s['max_seconds']:.6f} s",
        '100,836 input rows; 9,724 groups. Exact per-movie counts and full typed CSV/Parquet row multisets matched.'])
for label,key in [('Before index','before_index'),('After movie_id index','after_index')]:
    s = mongo[key]
    experiment_rows.append(['E3',label,
       f"Plan: {' + '.join(s['plan_stages'])}\nnReturned={s['nReturned']}; docsExamined={s['totalDocsExamined']}\nexecutionTimeMillis={s['executionTimeMillis']}; keysExamined={s['totalKeysExamined']}",
       'Same {movie_id: 1} full-document query; identical results. Index used after creation.'])
caption('Table 4. Single consolidated E1–E3 experiment table (seconds unless specified).')
table(['Test','Condition','Observed metrics','Validation / interpretation'], experiment_rows,[.4,1.05,3.0,2.42],8.7)
heading('Interpretation and evidence limitations')
paragraph('E1 varies event volume N, not producer batch.size (Python batch_size). The producer does not explicitly vary that configuration, so no batching-parameter effect is established. Higher throughput at larger N is consistent with amortized local overhead, not proof of its cause; these are two workload sizes, not repeated statistical trials. Offsets are (4454, 4818, 728) and (23700, 17314, 18986), totaling 10,000 and 60,000. The user reports two runs per topic; offsets alone do not identify each run’s count or rule out other sends.')
paragraph('Evidence discrepancy: supplied cross-checks were 0.652027 s / 7,668.39 records/s and 3.012877 s / 9,957.26 records/s. Neither pair occurs in the saved E1 JSON/TXT; this report uses the agreeing repository values in Table 4. Those alternative runs and a topic-description log are missing, so their exact provenance cannot be verified.')
paragraph(f"E2’s mean difference is {spark['comparison']['csv_mean_minus_parquet_mean_seconds']:.6f} s; CSV/Parquet mean ratio is {spark['comparison']['csv_mean_divided_by_parquet_mean']:.4f}× and median ratio is {spark['comparison']['csv_median_divided_by_parquet_median']:.4f}×. The measurement includes DataFrame construction through collect(), with explicit CSV schema and Parquet schema metadata reading. Startup, export, validation and result comparison are excluded. Both formats receive one warm-up, no DataFrame cache/persist calls, and three timed runs (CSV then Parquet); earlier scans may warm HDFS/OS caches.")
paragraph(f"E3 reduced examined documents by (1297−1)/1297 × 100 = {reduction:.3f}%. A collection scan became an index lookup plus document fetch. The 0 ms before / 1 ms after result demonstrates why these tiny timings cannot establish either a general improvement or regression. Spark’s recorded hostname fallback and native-Hadoop-library fallback did not prevent successful validation; their timing contribution was not isolated.")
source_line('E1: logs/e1/ratings-5k.{json,txt}, ratings-30k.{json,txt}, kafka-offsets.txt. E2: report/spark/benchmark_results.csv and benchmark_summary.json. E3: report/mongodb/benchmark_results.json and both complete explain JSON files.')

# Page 5
doc.add_page_break()
heading('Questions',1)
question(1, 'Logical size is the number of file-content bytes: 2,483,723 for ratings.csv and 494,431 for movies.csv. The configured HDFS block size is 134,217,728 bytes (128 MiB), a block allocation limit rather than either file’s actual length. Replication factor 1 means one replica of each block; fsck directly shows one block per file with Live_repl=1. A single DataNode with one replica has no alternate data copy if that node fails, so the HEALTHY result does not demonstrate fault tolerance (logs/hdfs/fsck-raw.txt).')
question(2, 'Using userId as the key consistently routes a user’s events to the same partition while the partition count and partitioner remain unchanged. Kafka provides an order within each partition; the three saved partition offsets per topic demonstrate the partitioned layout. It provides no single total ordering across the three partitions, and a consumer cannot infer global CSV row order from them. The measured producer uses keyed sends and acks="all"; delivery acknowledgement does not itself establish end-to-end exactly-once processing (kafka/producer.py; logs/e1/kafka-offsets.txt).')
question(3, 'Both the original CSV and typed Parquet contain 100,836 ratings and yield 9,724 movie groups. Every warm-up and timed query matched the reference movieId-to-count mapping, and the full typed row multisets were also compared. Parquet changes physical representation rather than adding, removing or changing logical records, so the same typed groupBy/count must return the same counts. A mismatch would indicate an input, parsing, schema, export or query problem and would fail this pipeline’s validation (report/spark/benchmark_summary.json).')
question(4, f"In these trials CSV averaged 0.230397 s and Parquet 0.177646 s, a 1.2969× mean ratio; medians were 0.232674 s and 0.175566 s. Typed columnar storage and reading only movieId may contribute, whereas CSV requires parsing, but the experiment does not isolate those causes. Explicit CSV schemas exclude schema-inference overhead and Spark startup is outside the timers. Earlier reads, warm-ups and unflushed HDFS/OS caches, along with scheduling, JIT and the small workload, can influence the ranges in Table 4. Three trials show variability, not that Parquet is always faster (report/spark/benchmark_results.csv).")
question(5, f"The winning plan changes from COLLSCAN to IXSCAN with FETCH after creating movie_id_1. totalDocsExamined decreases from 1,297 to 1 ({reduction:.3f}%), while nReturned remains 1 and the full returned document is identical. The selective key lookup avoids examining nonmatching documents, and FETCH retrieves the complete matching document. Server timings of 0 ms and 1 ms have coarse resolution and reflect single small, warm-collection observations, so they establish neither a general latency benefit nor a regression (report/mongodb/explain_before_index.json and explain_after_index.json).")

# Page 6
doc.add_page_break()
question(6, 'No: Presto is a distributed SQL query engine, here deployed as a single coordinator that also executes work. MongoDB remains the persistent source of the 1,297 summary documents; the MongoDB connector translates metadata access and supported SQL operations into MongoDB requests, while Presto performs remaining work. The top-ten SQL returned ten rows and all four fields matched the equivalently ranked MongoDB documents. Presto reads through the connector without permanently copying the entire collection into its own storage; connector _schema metadata is distinct from the movie data (report/presto/verification_results.json).')
question(7, 'If an import is rerun without clearing data or idempotent upserts, the same logical movie can be inserted again with a new _id. Counts and downstream queries may then include duplicate movie summaries; the nonunique movie_id_1 index alone does not prevent this. The implemented repeatable method validates all input first, drops only the dedicated bdsp_hw3.movie_stats collection and its indexes, reinserts the batch, and revalidates all 1,297 documents. An alternative is a unique movie_id index with replace_one(..., upsert=True) for each key, plus handling records removed from a later source snapshot (mongodb/import_and_benchmark.py; report/mongodb/benchmark_results.json).')
question(8, 'HDFS provides durable raw and curated file storage; Kafka provides persisted, partitioned message transport for ratings replay. Spark performs batch computation, while MongoDB durably serves the derived movie documents. Presto performs interactive SQL through the MongoDB connector; it is not another persistent copy of the collection. One-machine deployment is the key limitation: HDFS replication 1 and Kafka replication 1 provide no independent-node redundancy, and all services compete for the measured 6.64 GiB of RAM. These role boundaries are reflected in Figure 1, the two HDFS input files, the E1 topic offsets and the verified MongoDB-backed SQL results.')
heading('Reproducibility note',1)
repro = ('Download the official MovieLens Latest Small ZIP and preserve its CSV files. Start HDFS manually, set replication to one, and upload ratings.csv and movies.csv to /user/bdsp/hw3/raw; compare counts and fsck output. Start Kafka manually, delete and recreate only ratings-5k and ratings-30k with three partitions and replication one, then run kafka/producer.py with --topic and --num-records 5000 or 30000; verify starting and ending offsets. Run bash scripts/run_spark_hw3.sh for exports, warm-ups and three trials per format. Start MongoDB manually and run bash scripts/run_mongodb_hw3.sh; it validates input before resetting only bdsp_hw3.movie_stats and recreating the index experiment. Start Presto after MongoDB on their shared network, ensure connector metadata exists, then run bash scripts/run_presto_hw3.sh. Retain timestamped logs and report archives, and record active services before each phase.')
assert 100 <= len(repro.split()) <= 150, len(repro.split())
paragraph(repro)
source_line('Regenerate this DOCX with .venv/bin/python scripts/generate_hw3_report.py; dependencies: report/report_requirements.txt. No service commands or experiments run during report generation. Project commands resolve the existing .venv; raw HDFS data must not be reset accidentally.')
paragraph('Evidence gaps remain explicit: alternative E1 cross-check timings have no saved run artifact; broker topic-description output is absent; complete historical service inventories are unavailable outside the recorded checks. Docker runtime/image details are taken from configurations and prior logs, not new Docker inspections. The Word document is editable; no PDF is generated at this stage.')

output = REPORT / 'BDSP_HW3_Report.docx'
doc.save(output)
# Remove python-docx's template page-count cache: it is not a rendered count.
with zipfile.ZipFile(output) as z:
    contents = {name: z.read(name) for name in z.namelist()}
app = ET.fromstring(contents['docProps/app.xml'])
for child in list(app):
    if child.tag.endswith('}Pages'):
        app.remove(child)
contents['docProps/app.xml'] = ET.tostring(app, encoding='utf-8', xml_declaration=True)
with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as z:
    for name, content in contents.items():
        z.writestr(name, content)
check = Document(output)
text = '\n'.join(p.text for p in check.paragraphs)
assert all(questions[n] in text for n in range(1,9))
assert len(check.tables) == 4 and len(check.inline_shapes) == 1
breaks = check._element.xpath('//w:br[@w:type="page"]')
assert len(breaks) == 5
assert [p.text for p in check.paragraphs if p.style.name == 'Heading 1'] == [
    'Environment and data','Implementation','Experiments','Questions','Reproducibility note']
manifest = {path: hashlib.sha256((ROOT/path).read_bytes()).hexdigest() for path in sorted(set(SOURCES))}
validation = {
    'output': str(output.relative_to(ROOT)), 'planned_pages': 6, 'explicit_page_breaks': len(breaks),
    'rendered_page_count': None, 'pdf_created': False,
    'pagination_limit': 'No local Word/LibreOffice renderer; six designed pages, overflow not verified. No PDF generated per user instruction.',
    'tables': len(check.tables), 'figures': len(check.inline_shapes),
    'questions_verbatim': questions, 'answers_present': sorted(answers),
    'reproducibility_word_count': len(repro.split()),
    'sources_sha256': manifest,
    'evidence_gaps': ['Supplied E1 cross-check values differ from both saved JSON/TXT run pairs.',
                      'Kafka topic-description output is absent; topic replication is configuration evidence.',
                      'Complete historical service inventories were not recorded.'],
    'derived': {'mongodb_documents_examined_reduction_percent': reduction, 'offset_totals': offset_totals},
}
(REPORT/'report_validation.json').write_text(json.dumps(validation, indent=2)+'\n')
print(json.dumps({key: value for key,value in validation.items() if key not in ('sources_sha256','questions_verbatim')},indent=2))
