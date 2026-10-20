"""Compare saved Superset charts with the independently computed CSV totals."""
import json
import math
from pathlib import Path
import runpy
import sys

def main():
    expected=json.load(sys.stdin)['sensors'];tools=runpy.run_path('/usr/local/bin/apache-superset-verify');request=tools['request'];session=tools['wait']()
    dashboards=request(session,'GET','/api/v1/dashboard/').json()['result'];dashboard=next(v for v in dashboards if v['slug']=='data-platform-measurements')
    charts=request(session,'GET','/api/v1/dashboard/'+str(dashboard['id'])+'/charts').json()['result']
    checked=set()
    for chart in charts:
        detail=request(session,'GET','/api/v1/chart/'+str(chart['id'])).json()['result']
        context=detail['query_context'];context=json.loads(context) if isinstance(context,str) else context;context['force']=True
        result=request(session,'POST','/api/v1/chart/data',json=context).json()['result'][0]
        if result.get('error'):raise RuntimeError('Saved chart SQL failed')
        records={v['sensor']:v for v in result['data'] if v['sensor'] in expected}
        if set(records)!=set(expected):raise RuntimeError('CSV sensors missing from saved chart')
        for sensor,values in expected.items():
            row=records[sensor]
            if detail['viz_type']=='table':
                if row['sample_count']!=values['count']:raise RuntimeError('Dashboard count differs from CSV')
                if not math.isclose(row['total'],values['sum'],abs_tol=1e-9):raise RuntimeError('Dashboard sum differs from CSV')
                if not math.isclose(row['average'],values['mean'],abs_tol=1e-9):raise RuntimeError('Dashboard mean differs from CSV')
            elif detail['viz_type']=='pie':
                metrics=[value for key,value in row.items() if key!='sensor']
                if len(metrics)!=1 or metrics[0]!=values['count']:raise RuntimeError('Pie chart count differs from CSV')
            else:raise RuntimeError('Unexpected dashboard chart type')
        checked.add(detail['viz_type'])
    if checked!={'table','pie'}:raise RuntimeError('Both saved charts must be verified')
    print('DATA_PLATFORM_SUPERSET_CSV_TOTALS_OK')
if __name__=='__main__':
    try:main()
    except Exception as error:
        print('DASHBOARD_CHECK_FAILED: '+(str(error) if type(error) is RuntimeError else type(error).__name__),file=sys.stderr);sys.exit(1)
