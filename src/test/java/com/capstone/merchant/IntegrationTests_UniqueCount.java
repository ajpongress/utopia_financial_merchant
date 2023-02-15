package com.capstone.merchant;

import com.capstone.merchant.Configurations.BatchConfigMerchant;
import com.capstone.merchant.Configurations.BatchConfigUniqueCount;
import com.capstone.merchant.Models.MerchantModel;
import com.capstone.merchant.Processors.MerchantProcessor;
import com.capstone.merchant.Processors.UniqueCountProcessor;
import com.capstone.merchant.Readers.MerchantReaderCSV;
import com.capstone.merchant.TaskExecutors.TaskExecutor;
import com.capstone.merchant.Writers.MerchantWriterXML;
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

@SpringBatchTest
@SpringJUnitConfig(classes = {
        BatchConfigUniqueCount.class,
        MerchantModel.class,
        MerchantReaderCSV.class,
        UniqueCountProcessor.class,
        MerchantWriterXML.class,
        TaskExecutor.class
})
@EnableAutoConfiguration

public class IntegrationTests_UniqueCount {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobRepositoryTestUtils jobRepositoryTestUtils;

    private String INPUT = "src/test/resources/input/test_input.csv";

    @AfterEach
    public void cleanUp() {
        jobRepositoryTestUtils.removeJobExecutions();
    }

    private JobParameters testJobParameters_UniqueCount() {

        return new JobParametersBuilder()
                .addString("file.input", INPUT)
                .toJobParameters();
    }



    // ----------------------------------------------------------------------------------
    // --                                 TESTS                                        --
    // ----------------------------------------------------------------------------------

    @Test
    public void testBatchProcessFor_UniqueCount() throws Exception {

        // Load job parameters and launch job through test suite
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(testJobParameters_UniqueCount());
        JobInstance actualJobInstance = jobExecution.getJobInstance();
        ExitStatus actualJobExitStatus = jobExecution.getExitStatus();

        // ----- Assertions -----
        File testInputFile = new File(INPUT);

        // Match job names
        Assertions.assertEquals("getUniqueCountJob", actualJobInstance.getJobName());

        // Match job exit status to "COMPLETED"
        Assertions.assertEquals("COMPLETED", actualJobExitStatus.getExitCode());

        // Verify input file is valid and can be read
        Assertions.assertTrue(FileUtil.canReadFile(testInputFile));
    }


}
