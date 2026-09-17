import argparse, json
from datetime import datetime, timezone
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

WEATHER_WEIGHT = 0.45
AIR_WEIGHT = 0.35
LOGISTICS_WEIGHT = 0.20

def clamp(v): return max(0.0, min(100.0, float(v or 0)))
def weather_risk(severity):
    return {'extreme':100,'severe':80,'moderate':50,'minor':25}.get(str(severity or '').lower(),0)
def air_quality_risk(pm25): return clamp(float(pm25 or 0) / 75 * 100)
def logistics_risk(delay_rate): return clamp(float(delay_rate or 0) * 100)
def band(score): return 'LOW' if score < 25 else 'MODERATE' if score < 50 else 'HIGH' if score < 75 else 'CRITICAL'

def calculate_risk(r):
    score = WEATHER_WEIGHT*weather_risk(r.get('weather_severity')) + AIR_WEIGHT*air_quality_risk(r.get('pm25')) + LOGISTICS_WEIGHT*logistics_risk(r.get('delay_rate'))
    r['risk_score'] = round(clamp(score),2)
    r['risk_band'] = band(r['risk_score'])
    r['processed_at'] = datetime.now(timezone.utc).isoformat()
    return r

class ParseJSON(beam.DoFn):
    def process(self, value):
        try:
            if isinstance(value, bytes): value=value.decode()
            yield json.loads(value)
        except Exception as e:
            yield beam.pvalue.TaggedOutput('dead_letter', {'raw':str(value),'error':str(e)})

def run(argv=None):
    p=argparse.ArgumentParser()
    p.add_argument('--mode',choices=['batch','streaming'],default='batch')
    p.add_argument('--input')
    p.add_argument('--pubsub_subscription')
    p.add_argument('--output',required=True)
    known, pipeline_args=p.parse_known_args(argv)
    options=PipelineOptions(pipeline_args,save_main_session=True)
    options.view_as(StandardOptions).streaming=(known.mode=='streaming')
    with beam.Pipeline(options=options) as pipe:
        if known.mode=='streaming':
            source=pipe|'ReadPubSub'>>beam.io.ReadFromPubSub(subscription=known.pubsub_subscription)
        else:
            source=pipe|'ReadBronze'>>beam.io.ReadFromText(known.input)
        parsed=source|'ParseJSON'>>beam.ParDo(ParseJSON()).with_outputs('dead_letter',main='valid')
        parsed.valid|'CalculateRisk'>>beam.Map(calculate_risk)|'WriteSilver'>>beam.io.WriteToText(known.output,file_name_suffix='.jsonl')
        parsed.dead_letter|'WriteDLQ'>>beam.io.WriteToText(known.output+'_dead_letter',file_name_suffix='.jsonl')

if __name__=='__main__': run()
