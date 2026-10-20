"""Root-owned local health collection. Exports numeric metrics, never credentials."""
import json
import os
from pathlib import Path
import ssl
import subprocess
import time
import urllib.request
CONFIG=Path('/etc/data-platform-monitor.json')
OUTPUT=Path('/var/lib/prometheus/node-exporter/data-platform.prom')
def main():
    config=json.loads(CONFIG.read_text());lines=[];success=1
    def metric(name,value,labels=''):lines.append(name+labels+' '+str(float(value)))
    for service in config['services']:
        active=subprocess.run(['systemctl','is-active','--quiet',service],capture_output=True).returncode==0
        metric('platform_service_up',active,'{service="'+service+'"}')
    def get(url,**kw):
        with urllib.request.urlopen(url,timeout=10,**kw) as response:return json.load(response)
    try:
        if config['component']=='apache_pulsar_vm':
            status=json.loads(Path('/var/lib/data-platform-analytics/status.json').read_text())
            for name in ('stored','duplicates','quarantined','failures'):metric('platform_analytics_'+name+'_total',status.get(name,0))
            metric('platform_analytics_updated_timestamp_seconds',status['updated_at'])
            token=Path('/etc/apache-pulsar/admin.token').read_text().strip()
            context=ssl.create_default_context(cafile='/etc/apache-pulsar/tls/server.crt')
            for topic,subscriptions in [('data-platform',['dcache-ingest','clickhouse-analytics']),('measurements',['flink-measurements'])]:
                url='https://'+config['host']+':8443/admin/v2/persistent/platform/events/'+topic+'/stats'
                stats=get(urllib.request.Request(url,headers={'Authorization':'Bearer '+token}),context=context)
                for subscription in subscriptions:
                    state=stats['subscriptions'][subscription]
                    labels='{subscription="'+subscription+'"}'
                    metric('platform_pulsar_subscription_backlog',state['msgBacklog'],labels)
                    metric('platform_pulsar_subscription_unacked',state.get('unackedMessages',0),labels)
        elif config['component']=='apache_flink_vm':
            job='bd003e1f1db333f8652cb8a391d5ab3a';base='http://127.0.0.1:8081/jobs/'+job
            metric('platform_flink_job_running',get(base)['state']=='RUNNING')
            checkpoints=get(base+'/checkpoints');completed=checkpoints.get('latest',{}).get('completed')
            metric('platform_flink_completed_checkpoints_total',checkpoints['counts']['completed'])
            metric('platform_flink_last_checkpoint_timestamp_seconds',(completed or {}).get('latest_ack_timestamp',0)/1000)
    except Exception:success=0
    metric('platform_probe_success',success);metric('platform_probe_timestamp_seconds',time.time())
    temporary=OUTPUT.with_suffix('.tmp');temporary.write_text('\n'.join(lines)+'\n');temporary.chmod(0o644);temporary.replace(OUTPUT)
if __name__=='__main__':main()
