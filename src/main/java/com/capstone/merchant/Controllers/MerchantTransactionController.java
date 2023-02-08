package com.capstone.merchant.Controllers;

import com.capstone.merchant.Services.MerchantTransactionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MerchantTransactionController {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    @Autowired
    MerchantTransactionService merchantTransactionService;



    // ----------------------------------------------------------------------------------
    // --                               MAPPINGS                                       --
    // ----------------------------------------------------------------------------------

    // Export all merchants
    @GetMapping("/merchants")
    public ResponseEntity<String> allMerchantsAPI(@RequestParam String source, @RequestParam String destination) {

        return merchantTransactionService.exportAllMerchants(source, destination);
    }

    // Export specific merchantID (merchant name in CSV)
    @GetMapping("/merchants/{merchantID}")
    public ResponseEntity<String> oneMerchantAPI(@PathVariable long merchantID, @RequestParam String source, @RequestParam String destination) {

        return merchantTransactionService.exportSingleMerchant(merchantID, source, destination);
    }
}
