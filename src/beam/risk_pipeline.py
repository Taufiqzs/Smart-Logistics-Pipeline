import argparse, json
from datetime import datetime, timezone
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# Bobot komponen risiko cuaca dalam perhitungan skor akhir.
WEATHER_WEIGHT = 0.45
# Bobot komponen risiko kualitas udara dalam perhitungan skor akhir.
AIR_WEIGHT = 0.35
# Bobot komponen risiko logistik dalam perhitungan skor akhir.
LOGISTICS_WEIGHT = 0.20

# Membatasi nilai agar tetap berada dalam rentang minimum dan maksimum.
def clamp(v): return max(0.0, min(100.0, float(v or 0)))
# Menghasilkan skor risiko cuaca dari tingkat keparahan cuaca.
def weather_risk(severity):
    return {'extreme':100,'severe':80,'moderate':50,'minor':25}.get(str(severity or '').lower(),0)
# Mengubah nilai PM2.5 menjadi skor risiko kualitas udara proyek.
def air_quality_risk(pm25): return clamp(float(pm25 or 0) / 75 * 100)
# Mengubah tingkat keterlambatan logistik menjadi skor risiko.
def logistics_risk(delay_rate): return clamp(float(delay_rate or 0) * 100)
# Mengubah skor risiko numerik menjadi kategori risiko.
def band(score): return 'LOW' if score < 25 else 'MODERATE' if score < 50 else 'HIGH' if score < 75 else 'CRITICAL'

# Menghitung skor risiko gabungan dari cuaca, kualitas udara, dan logistik.
def calculate_risk(r):
# Menyimpan skor risiko yang telah dihitung.
    score = (
        WEATHER_WEIGHT * weather_risk(
            r.get('severity', r.get('weather_severity'))
        )
        + AIR_WEIGHT * air_quality_risk(r.get('pm25'))
        + LOGISTICS_WEIGHT * logistics_risk(r.get('delay_rate'))
    )    
    r['risk_score'] = round(clamp(score),2)
    r['risk_band'] = band(r['risk_score'])
    r['processed_at'] = datetime.now(timezone.utc).isoformat()
    return r

# Kelas `ParseJSON` mengubah pesan JSON menjadi dictionary Python untuk diproses oleh Apache Beam.
class ParseJSON(beam.DoFn):
# Fungsi `process()` menjalankan pemrosesan setiap elemen data pada pipeline.
    def process(self, value):
        try:
# Menyimpan nilai yang dibaca dari setiap record.
            if isinstance(value, bytes): value=value.decode()
            yield json.loads(value)
        except Exception as e:
            yield beam.pvalue.TaggedOutput('dead_letter', {'raw':str(value),'error':str(e)})

# Membangun dan menjalankan pipeline Apache Beam/Dataflow.
def run(argv=None):
# Menyimpan objek pipeline Apache Beam.
    p=argparse.ArgumentParser()
    p.add_argument('--mode',choices=['batch','streaming'],default='batch')
    p.add_argument('--input')
    p.add_argument('--pubsub_subscription')
    p.add_argument('--output',required=True)
# Menyimpan argumen pipeline yang sudah dikenali dan argumen tambahan untuk eksekusi.
    known, pipeline_args=p.parse_known_args(argv)
# Menyimpan konfigurasi eksekusi Apache Beam.
    options=PipelineOptions(pipeline_args,save_main_session=True)
# Mengaktifkan mode streaming jika pipeline dijalankan sebagai streaming.
    options.view_as(StandardOptions).streaming=(known.mode=='streaming')
    with beam.Pipeline(options=options) as pipe:
        if known.mode=='streaming':
# Menyimpan sumber data yang dibaca oleh pipeline.
            source=pipe|'ReadPubSub'>>beam.io.ReadFromPubSub(subscription=known.pubsub_subscription)
        else:
# Menyimpan sumber data yang dibaca oleh pipeline.
            source=pipe|'ReadBronze'>>beam.io.ReadFromText(known.input)
# Menyimpan record yang sudah diubah dari JSON menjadi dictionary.
        parsed=source|'ParseJSON'>>beam.ParDo(ParseJSON()).with_outputs('dead_letter',main='valid')
        parsed.valid|'CalculateRisk'>>beam.Map(calculate_risk)|'WriteSilver'>>beam.io.WriteToText(known.output,file_name_suffix='.jsonl')
        parsed.dead_letter|'WriteDLQ'>>beam.io.WriteToText(known.output+'_dead_letter',file_name_suffix='.jsonl')

if __name__=='__main__': run()
