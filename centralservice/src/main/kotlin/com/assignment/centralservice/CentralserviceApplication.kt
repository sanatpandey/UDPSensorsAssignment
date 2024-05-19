package com.assignment.centralservice

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory

@SpringBootApplication
class CentralserviceApplication{
	@KafkaListener(topics = ["sensor_data"], groupId = "my-consumer-group")
	fun processSensorData(message: String) {
		val parts = message.split("=")
		if (parts.size == 2) {
			val sensorId = parts[0]
			val value: Int? = parts[1].replace("\n", "").toIntOrNull()
			if(sensorId.startsWith("t")){
				handleTempAlert(value)
			}else if(sensorId.startsWith("h")){
				handleHumidityAlert(value)
			}else{
				println("Error: Unidentified Data")
			}
		}
	}

	private fun handleHumidityAlert(value: Int?) {
		value?.let {
			if (it > 50){
				println("Alarm: Sensor Humidity has value $it% which exceeds threshold.")
			}else{
				println("Weather is good")
			}
		}
	}

	private fun handleTempAlert(value: Int?) {
		value?.let {
			if (it > 35){
				println("Alarm: Sensor Temerature has value $it which exceeds threshold.")
			}else{
				println("Weather is good")
			}
		}
	}
}

fun main(args: Array<String>) {
	runApplication<CentralserviceApplication>(*args)
}
