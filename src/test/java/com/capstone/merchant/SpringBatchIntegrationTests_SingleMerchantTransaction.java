package com.capstone.merchant;

// ********************************************************************************
//                          Test Single Merchant Transaction Operations
// ********************************************************************************

import com.capstone.merchant.Classifiers.MerchantTransactionClassifier;
import com.capstone.merchant.Configurations.BatchConfigSingleMerchant;
import com.capstone.merchant.Models.MerchantTransactionModel;
import com.capstone.merchant.Processors.SingleMerchantProcessor;
import com.capstone.merchant.Readers.MerchantTransactionReaderCSV;
import com.capstone.merchant.TaskExecutors.TaskExecutor;
import com.capstone.merchant.Writers.MerchantTransactionCompositeWriter;
import org.apache.commons.io.FileUtils;
import org.aspectj.util.FileUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.*;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.io.File;

// ********************************************************************************
//                          Test Single Merchant Operations
// ********************************************************************************

@SpringBatchTest
@SpringJUnitConfig(classes = {
        BatchConfigSingleMerchant.class,
        MerchantTransactionClassifier.class,
        MerchantTransactionModel.class,
        MerchantTransactionReaderCSV.class,
        SingleMerchantProcessor.class,
        MerchantTransactionCompositeWriter.class,
        TaskExecutor.class
})
@EnableAutoConfiguration

public class SpringBatchIntegrationTests_SingleMerchantTransaction {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobRepositoryTestUtils jobRepositoryTestUtils;

    // Set merchantID to test for single merchant operations & export
    private long merchantID = -3527213246127876953L;
    private String INPUT = "src/test/resources/input/test_input.csv";
    private String EXPECTED_OUTPUT = "src/test/resources/output/expected_output_SingleMerchantTransaction.xml";
    private String ACTUAL_OUTPUT = "src/test/resources/output/merchant_" + merchantID;

    @AfterEach
    public void cleanUp() {
        jobRepositoryTestUtils.removeJobExecutions();
    }

    private JobParameters testJobParameters_SingleMerchantTransaction() {

        return new JobParametersBuilder()
                .addLong("merchantID_param", merchantID)
                .addString("file.input", INPUT)
                .addString("outputPath_param", ACTUAL_OUTPUT)
                .toJobParameters();
    }



    // ----------------------------------------------------------------------------------
    // --                                 TESTS                                        --
    // ----------------------------------------------------------------------------------

    @Test
    public void testBatchProcessFor_SingleMerchantTransaction() throws Exception {

        // Load job parameters and launch job through test suite
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(testJobParameters_SingleMerchantTransaction());
        JobInstance actualJobInstance = jobExecution.getJobInstance();
        ExitStatus actualJobExitStatus = jobExecution.getExitStatus();

        // ----- Assertions -----
        File testInputFile = new File(INPUT);
        File testOutputFileExpected = new File(EXPECTED_OUTPUT);
        File testOutputFileActual = new File(ACTUAL_OUTPUT + "/merchant_" + merchantID + "_transactions.xml");

        // Match job names
        Assertions.assertEquals("exportSingleMerchantJob", actualJobInstance.getJobName());

        // Match job exit status to "COMPLETED"
        Assertions.assertEquals("COMPLETED", actualJobExitStatus.getExitCode());

        // Verify input file is valid and can be read
        Assertions.assertTrue(FileUtil.canReadFile(testInputFile));

        // Verify output (expected) file is valid and can be read
        Assertions.assertTrue(FileUtil.canReadFile(testOutputFileExpected));

        // Verify output (actual) file is valid and can be read
        Assertions.assertTrue(FileUtil.canReadFile(testOutputFileActual));

        // Verify expected and actual output files match
        Assertions.assertEquals(
                FileUtils.readFileToString(testOutputFileExpected, "utf-8"),
                FileUtils.readFileToString(testOutputFileActual, "utf-8"),
                "============================== FILE MISMATCH ==============================");
    }
}
