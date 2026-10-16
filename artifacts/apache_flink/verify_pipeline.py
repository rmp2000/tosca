"""Run on the Pulsar VM; verify real Flink results stored by the dCache bridge."""
import hashlib
import json
from pathlib import Path
import ssl
import sys
import time
import urllib.request
import uuid

sys.path.insert(0, '/usr/local/lib/data-platform-ingest')
import consumer

STATE = Path('/srv/apache-pulsar/.flink-flow-verification.json')


def wait_for(test, label, timeout=240):
    deadline = time.monotonic()+timeout
    while time.monotonic() < deadline:
        try:
            result = test()
            if result:
                return result
        except Exception:
            pass
        time.sleep(1)
    raise RuntimeError('Timed out: '+label)


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def main():
    mode = sys.argv[1]
    endpoints = json.load(sys.stdin)
    config = json.loads(consumer.CONFIG.read_text())
    if endpoints['output_topic'] != config['topic']:
        raise RuntimeError('Output topic does not match the running storage bridge')
    storage = consumer.WebDAV(config)
    token = Path('/etc/apache-pulsar/admin.token').read_text().strip()
    context = ssl.create_default_context(cafile=config['pulsar_ca'])

    def backlog(topic, subscription):
        path = '/admin/v2/persistent/'+topic.removeprefix('persistent://')+'/stats'
        request = urllib.request.Request(config['admin_url']+path,
                                         headers={'Authorization': 'Bearer '+token})
        with urllib.request.urlopen(request, context=context, timeout=10) as response:
            state = json.load(response)['subscriptions'][subscription]
            return state['msgBacklog']+state.get('unackedMessages', 0)

    def drained():
        return (backlog(endpoints['input_topic'], endpoints['subscription']) == 0
                and backlog(config['topic'], config['subscription']) == 0)

    def publish(records):
        client = consumer.open_client(config)
        try:
            producer = client.create_producer(endpoints['input_topic'], batching_enabled=False)
            for record in records:
                producer.send(record.encode())
        finally:
            client.close()

    def key(event_id):
        return consumer.object_key(config['topic'], {'event_id': event_id}, b'')

    def result_id(sensor, event_id):
        return 'measurement-'+digest(sensor+'\0'+event_id)

    def expect(event_id, expected):
        payload = storage.get(key(event_id))
        if payload is None:
            return False
        value = json.loads(payload)
        return all(value.get(name) == expected_value for name, expected_value in expected.items())

    keys = []
    if mode == 'check':
        staged = json.loads(STATE.read_text())
        sensor, tag = staged['sensor'], staged['tag']
        keys.extend(staged['keys'])
        third = result_id(sensor, tag+'_3')
        keys.append(key(third))
        publish([json.dumps({'sensor': sensor, 'event_id': tag+'_2', 'value': 20}),
                 json.dumps({'sensor': sensor, 'event_id': tag+'_3', 'value': 30})])
        wait_for(lambda: expect(third, {'record_type': 'measurement_summary', 'sensor': sensor,
                                        'count': 3, 'sum': 60, 'mean': 20}), 'restored Flink aggregate in dCache')
        wait_for(drained, 'checkpointed source and acknowledged storage after recovery')
        print('DATA_PLATFORM_FLINK_STATE_RECOVERY_OK', flush=True)
    elif mode in ('smoke', 'stage'):
        tag = uuid.uuid4().hex
        sensor = 'verify_'+tag
        first = result_id(sensor, tag+'_1')
        second = result_id(sensor, tag+'_2')
        invalid = 'invalid-json-'+tag
        conflict = json.dumps({'sensor': sensor, 'event_id': tag+'_2', 'value': 99})
        rejected_invalid = 'rejected-'+digest(invalid)
        rejected_conflict = 'rejected-'+digest(conflict)
        keys = [key(item) for item in (first, second, rejected_invalid, rejected_conflict)]
        records = [json.dumps({'sensor': sensor, 'event_id': tag+'_1', 'value': 10}),
                   json.dumps({'sensor': sensor, 'event_id': tag+'_2', 'value': 20}),
                   json.dumps({'sensor': sensor, 'event_id': tag+'_2', 'value': 20}),
                   invalid, conflict]
        if mode == 'stage':
            STATE.write_text(json.dumps({'sensor': sensor, 'tag': tag, 'keys': keys}))
            STATE.chmod(0o600)
        publish(records)
        wait_for(lambda: expect(first, {'sensor': sensor, 'count': 1, 'sum': 10, 'mean': 10}), 'first measurement result')
        wait_for(lambda: expect(second, {'sensor': sensor, 'count': 2, 'sum': 30, 'mean': 15}), 'count, sum and mean in dCache')
        wait_for(lambda: expect(rejected_invalid, {'record_type': 'rejected', 'reason': 'invalid_measurement'}), 'invalid input rejection')
        wait_for(lambda: expect(rejected_conflict, {'record_type': 'rejected', 'reason': 'conflicting_event_id'}), 'conflicting duplicate rejection')
        wait_for(drained, 'duplicate ignored, source checkpointed, and storage acknowledged')
        print('DATA_PLATFORM_FLINK_FLOW_OK', flush=True)
        if mode == 'stage':
            print('DATA_PLATFORM_FLINK_RECOVERY_STAGED', flush=True)
            return
    else:
        raise RuntimeError('Unknown verification mode')
    for name in keys:
        storage.delete(name)
        if storage.get(name) is not None:
            raise RuntimeError('Flink fixture cleanup failed')
    if mode == 'check':
        STATE.unlink()


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        label = str(error) if type(error) is RuntimeError else type(error).__name__
        print('Flink flow verification failed: '+label, file=sys.stderr)
        raise SystemExit(1)
