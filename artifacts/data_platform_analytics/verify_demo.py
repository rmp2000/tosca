"""Replay attributed CSV through Flink, compare SQL aggregates and exact dCache bytes."""
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import sys
import ssl
import urllib.request
import time
sys.path.insert(0,'/usr/local/lib/data-platform-analytics')
import consumer
import publisher
spec=importlib.util.spec_from_file_location('storage_consumer','/usr/local/lib/data-platform-ingest/consumer.py')
storage_consumer=importlib.util.module_from_spec(spec);spec.loader.exec_module(storage_consumer)

def main():
    config=json.loads(consumer.CONFIG.read_text());target=consumer.ClickHouse(config)
    archive_config=json.loads(storage_consumer.CONFIG.read_text());archive=storage_consumer.WebDAV(archive_config)
    records=publisher.records('/usr/local/share/data-platform/fixtures/iris-measurements.csv','iris_demo_v1')
    expected={};ids=[]
    for value in records:
        expected.setdefault(value['sensor'],[]).append(value['value'])
        ids.append('measurement-'+hashlib.sha256((value['sensor']+'\0'+value['event_id']).encode()).hexdigest())
    context=ssl.create_default_context(cafile=config['pulsar_ca'])
    admin=Path('/etc/apache-pulsar/admin.token').read_text().strip()
    def drained():
        for topic,subscription in [(config['input_topic'],'flink-measurements'),(config['topic'],config['subscription']),(archive_config['topic'],archive_config['subscription'])]:
            url=config['admin_url']+'/admin/v2/persistent/'+topic.removeprefix('persistent://')+'/stats'
            request=urllib.request.Request(url,headers={'Authorization':'Bearer '+admin})
            with urllib.request.urlopen(request,context=context,timeout=15) as response:state=json.load(response)['subscriptions'][subscription]
            if state['msgBacklog']+state.get('unackedMessages',0):return False
        return True
    def rows():
        values=[json.loads(line) for line in target.query("SELECT event_id,sensor,count,sum,mean,payload,payload_sha256 FROM analytics.events FINAL WHERE startsWith(sensor,'iris_demo_v1_') FORMAT JSONEachRow").splitlines()]
        for row in values:
            row['count']=int(row['count'])
            row['sum']=float(row['sum']);row['mean']=float(row['mean'])
        return values
    def verify():
        values=rows()
        if len(values)!=len(records) or {v['event_id'] for v in values}!=set(ids):return False
        for sensor,measurements in expected.items():
            latest=max((v for v in values if v['sensor']==sensor),key=lambda v:v['count'])
            if latest['count']!=len(measurements):return False
            if not math.isclose(latest['sum'],sum(measurements),abs_tol=1e-9):return False
            if not math.isclose(latest['mean'],sum(measurements)/len(measurements),abs_tol=1e-9):return False
        for value in values:
            key=storage_consumer.object_key(config['topic'],{'event_id':value['event_id']},b'')
            payload=archive.get(key)
            if payload is None or payload!=value['payload'].encode() or hashlib.sha256(payload).hexdigest()!=value['payload_sha256']:return False
        return values
    def wait():
        end=time.monotonic()+360
        while time.monotonic()<end:
            try:
                result=verify()
                if result:return result
            except Exception:pass
            time.sleep(2)
        raise RuntimeError('CSV aggregate or exact archive comparison timed out')
    client=consumer.open_client(config)
    try:
        producer=client.create_producer(config['input_topic'],batching_enabled=False)
        for iteration in range(2):
            for value in records:producer.send(json.dumps(value,allow_nan=False).encode())
            verified=wait()
        end=time.monotonic()+360
        while time.monotonic()<end:
            try:
                if drained():break
            except Exception:pass
            time.sleep(2)
        else:raise RuntimeError('Input source or independent output subscriptions did not drain after replay')
        verified=wait()
    finally:client.close()
    result={'dataset':'iris_demo_v1','input_records':len(records),'published_with_replay':2*len(records),'unique_output_records':len(verified),'sensors':{k:{'count':len(v),'sum':sum(v),'mean':sum(v)/len(v)} for k,v in sorted(expected.items())}}
    Path('/var/lib/data-platform-analytics/demo-verification.json').write_text(json.dumps(result,indent=2))
    print(json.dumps(result,sort_keys=True));print('DATA_PLATFORM_CSV_REPLAY_ARCHIVE_SQL_OK')
if __name__=='__main__':
    try:main()
    except Exception as error:
        print('CSV_CHECK_FAILED: '+(str(error) if type(error) is RuntimeError else type(error).__name__),file=sys.stderr);sys.exit(1)
