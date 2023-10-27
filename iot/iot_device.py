import json, time, random
from kafka import KafkaProducer
from kafka.errors import KafkaError

REDPANDA_TOPIC = 'telemetry-in'

def on_success(metadata):
  print(f"Sensor readings published to topic '{metadata.topic}' at offset {metadata.offset}")

def on_error(e):
  print(f"Error sending message: {e}")

def get_sensor_readings():
  
  readings = {}

  readings["sensor1_temp"] = random.randint(14,50)
  readings["sensor2_temp"] = random.randint(14,50)
  readings["sensor3_temp"] = random.randint(14,50)
  readings["sensor4_temp"] = random.randint(14,50)

  return readings

if __name__ == '__main__':

  try:

    producer = KafkaProducer(
      bootstrap_servers = "localhost:19092",
      value_serializer=lambda m: json.dumps(m).encode('ascii')
    )

    print("Publishing Sensor Readings")

    while True:
      
      current_readings = get_sensor_readings()
      print(f'Current readings {current_readings}')

      future = producer.send(REDPANDA_TOPIC,current_readings)
      
      future.add_callback(on_success)
      future.add_errback(on_error)

      time.sleep(5)

  except KeyboardInterrupt:
    print("Sensor Reading Publishing Interrupted")

  except (KafkaError, Exception) as e:
    print(" Exception ..",e)

  finally:
    producer.flush()
    producer.close()
