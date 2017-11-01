package com.foobaar.kafka.offset.manager.controller

import com.foobaar.kafka.offset.manager.dao.OffSetChangeRequest
import com.foobaar.kafka.offset.manager.service.KafkaOffsetService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

import org.springframework.web.bind.annotation.RequestMethod.POST

@RestController
class KafkaOffsetController {
    @Autowired private val service: KafkaOffsetService? = null

    @RequestMapping(value = "/custom-offset", method = arrayOf(POST))
    fun changeOffset(@RequestBody req: OffSetChangeRequest): String? {
        return service?.changeOffset(req)
    }

    @RequestMapping(value = "/boundary", method = arrayOf(POST))
    fun changeOffsetToEnd(@RequestBody req: OffSetChangeRequest,
                          @RequestParam position: String): String? {
        return if (!"start".equals(position, ignoreCase = true) && !"end".equals(position, ignoreCase = true)) {
            "position has to be one of the following start|end"
        } else service?.changeOffsetToEnd(req, position.toLowerCase())

    }

    @RequestMapping(value = "/time-based-offset", method = arrayOf(POST))
    fun changeOffsetBasedOnTimeStamp(@RequestBody req: OffSetChangeRequest): String? {
        return if (req.timeStampInMillis < 0) {
            "timeStampInMillis cannot be negative"
        } else service?.changeOffsetBasedOnTimeStamp(req)
    }
}
