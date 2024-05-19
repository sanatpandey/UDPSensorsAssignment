package com.assignment.warehouseservice

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.net.DatagramPacket
import java.net.DatagramSocket

@SpringBootApplication
class WarehouseService {
	@Autowired
	lateinit var kafkaTemplate: KafkaTemplate<String, String>

	@Bean
	fun sensorDataListener(): SensorDataListener {
		return SensorDataListener(this)
	}

	fun startListeningForSensors() {
		val socket = DatagramSocket(3344)
		val buffer = ByteArray(1024)
		while (true) {
			val packet = DatagramPacket(buffer, buffer.size)
			socket.receive(packet)
			val message = String(packet.data, 0, packet.length)
			parseAndSendToKafka(message)
			// Clear buffer for next message
			buffer.fill(0)
		}
	}

	fun parseAndSendToKafka(message: String) {
		// Assuming message format is sensor_id=t1 or h1; value=value;
		val parts = message.split(";")
		val sensorsData = getSensorData(parts)
		if (sensorsData.size == 2) {
			val sensorId = sensorsData[0]
			val value = sensorsData[1]
			kafkaTemplate.send("sensor_data", "$sensorId=$value")
		}
	}

	private fun getSensorData(data: List<String>): List<String> {
		val sensorDetails : MutableList<String> = arrayListOf()
		if(data.size == 2){
			for (item in data) {
				val parts = item.split("=")
				if (parts.size == 2) {
					sensorDetails.add(parts[1])
				}
			}
		}else{
			sensorDetails.add("error")
			sensorDetails.add("Undefined Data")
		}
		return sensorDetails
	}
}

@Component
class SensorDataListener(private val warehouseService: WarehouseService) : Runnable {
	override fun run() {
		warehouseService.startListeningForSensors()
	}
}

fun main(args: Array<String>) {
	val context = runApplication<WarehouseService>(*args)
	val warehouseService = context.getBean(WarehouseService::class.java)
	val sensorDataListener = warehouseService.sensorDataListener()
	val thread = Thread(sensorDataListener)
	thread.start()
}