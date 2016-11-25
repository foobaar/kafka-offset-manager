package com.foobaar.kafka.offset.manager.controller;

import com.foobaar.kafka.offset.manager.dao.OffSetChangeRequest;
import com.foobaar.kafka.offset.manager.service.KafkaOffsetService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import static org.springframework.web.bind.annotation.RequestMethod.POST;

@RestController
public class KafkaOffsetController {
  @Autowired private KafkaOffsetService service;

  @RequestMapping(value = "/custom-offset", method = POST)
  public String changeOffset(@RequestBody OffSetChangeRequest req) {
    return service.changeOffset(req);
  }

  @RequestMapping(value = "/boundary", method = POST)
  public String changeOffsetToEnd(@RequestBody OffSetChangeRequest req,
                                  @RequestParam String position) {
    if (!"start".equalsIgnoreCase(position) && !"end".equalsIgnoreCase(position)) {
      return "position has to be one of the following start|end";
    }

    return service.changeOffsetToEnd(req, position);
  }

}
