import matplotlib.pyplot as plt
import time, queue , json
from kafka import KafkaConsumer
from multiprocessing import Process, Queue
from kafka.errors import KafkaError

topic_queue = Queue()

def consume_data(q):
	
	try:

		print("Starting Kafka consumer")

		consumer = KafkaConsumer('telemetry-out',
			bootstrap_servers=["localhost:19092"],
		)

		print("Listening for messages in output topic")

		for message in consumer:

	  	
			value = json.loads(message.value)	    
			print(value['avg'])

			#Post the outgoing spark dataframe in the multiprocessing queue
			q.put(value)

	except KeyboardInterrupt:
	 	print("Consumer Interrupted")

	except (KafkaError, Exception) as e:
	 	print(" Exception ..",e)


if __name__ == '__main__':

  #Set axis queues for holding last ten dataframes
	qx = queue.Queue(maxsize=10)
	qy1 = queue.Queue(maxsize=10)
	qy2 = queue.Queue(maxsize=10)
	qy3 = queue.Queue(maxsize=10)
	qy4 = queue.Queue(maxsize=10)
	qy5 = queue.Queue(maxsize=10)

  #Set label for last ten records in x axis
	qx.put("r-9")
	qx.put("r-8")
	qx.put("r-7")
	qx.put("r-6")
	qx.put("r-5")
	qx.put("r-4")
	qx.put("r-3")
	qx.put("r-2")
	qx.put("r-1")
	qx.put("r")

  #Initialize y axis to zero
	for i in range(0,10):

		qy1.put(0)
		qy2.put(0)
		qy3.put(0)
		qy4.put(0)
		qy5.put(0)

  #Start kafka consumer process
	consume_process = Process(target=consume_data,args=(topic_queue,))
	consume_process.start()

  #Plot and refresh the grapf everytime new data received
	plt.ion()
	while True:
		
		try:

      #Receive data from multiprocessing queue
			item = topic_queue.get(block=False,timeout=5)
			print("item is " , item)

      #Shift the earliest record out of the queue to maintain last 10 dataframes for plotting
			qy1.get()
			qy1.put(int(item["sensor1_temp"]))

			qy2.get()
			qy2.put(int(item["sensor2_temp"]))

			qy3.get()
			qy3.put(int(item["sensor3_temp"]))

			qy4.get()
			qy4.put(int(item["sensor4_temp"]))

			qy5.get()
			qy5.put(float(item["avg"]))

		except queue.Empty:
			pass

		plt.gca().cla()

		plt.plot(qx.queue,qy1.queue,'ro')
		plt.plot(qx.queue,qy2.queue, 'bo')
		plt.plot(qx.queue,qy3.queue , 'yo')
		plt.plot(qx.queue,qy4.queue, 'go')
		plt.plot(qx.queue,qy5.queue)

		plt.xlabel("Records")
		plt.ylabel("Temperature")

		plt.title("IoT Telemetry")
		plt.legend(['Sensor 1','Sensor 2','Sensor 3','Sensor 4','Avg. Temp'],loc="upper left")
		#plt.figure(figsize=(400,250))
		plt.tight_layout()
		plt.show()

		plt.pause(0.1)
