package com.capstone.merchant.Controllers;

import com.capstone.merchant.Services.MerchantService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MerchantController {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    @Autowired
    MerchantService merchantService;



    // ----------------------------------------------------------------------------------
    // --                               MAPPINGS                                       --
    // ----------------------------------------------------------------------------------

    // generate state data
    @GetMapping("/generatemerchants")
    public ResponseEntity<String> generateStatesAPI(@RequestParam String source, @RequestParam String destination) {

        return merchantService.generateMerchants(source, destination);
    }
}
