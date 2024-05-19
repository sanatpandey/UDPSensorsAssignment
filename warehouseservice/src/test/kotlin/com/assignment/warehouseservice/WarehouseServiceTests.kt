package com.assignment.warehouseservice

import org.junit.jupiter.api.Test
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.kafka.core.KafkaTemplate

@SpringBootTest
class WarehouseServiceTests {

	@MockBean
	private lateinit var kafkaTemplate: KafkaTemplate<String, String>

	@Test
	fun testParseAndSendToKafka() {
		val warehouseService = WarehouseService()
		warehouseService.kafkaTemplate = kafkaTemplate

		`when`(kafkaTemplate.send("sensor_data", "t1=30")).thenReturn(null)

		warehouseService.parseAndSendToKafka("sensor_id=t1;value=30")

		verify(kafkaTemplate).send("sensor_data", "t1=30")
	}
}
