package com.capstone.merchant.Services;

import com.capstone.merchant.Configurations.BatchConfigMerchant;
import com.capstone.merchant.Configurations.BatchConfigUniqueCount;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.NoSuchElementException;

@Service
public class MerchantService {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    BatchConfigMerchant batchConfigMerchant;

    @Autowired
    BatchConfigUniqueCount batchConfigUniqueCount;

    private JobParameters buildJobParameters_Merchant(String pathInput, String pathOutput) {

        // Check if source file.input is valid
        File file = new File(pathInput);
        if (!file.exists()) {
            throw new ItemStreamException("Requested source doesn't exist");
        }

        return new JobParametersBuilder()
                .addLong("time.Started", System.currentTimeMillis())
                .addString("file.input", pathInput)
                .addString("outputPath_param", pathOutput)
                .toJobParameters();
    }

    // ----------------------------------------------------------------------------------

    private JobParameters buildJobParameters_UniqueCount(String pathInput) {

        // Check if source file.input is valid
        File file = new File(pathInput);
        if (!file.exists()) {
            throw new ItemStreamException("Requested source doesn't exist");
        }

        return new JobParametersBuilder()
                .addLong("time.Started", System.currentTimeMillis())
                .addString("file.input", pathInput)
                .toJobParameters();
    }



    // ----------------------------------------------------------------------------------
    // --                                METHODS                                       --
    // ----------------------------------------------------------------------------------

    // generate merchants
    public ResponseEntity<String> generateMerchants(String pathInput, String pathOutput) {

        try {
            JobParameters jobParameters = buildJobParameters_Merchant(pathInput, pathOutput);
            jobLauncher.run(batchConfigMerchant.job_generateMerchants(), jobParameters);

        } catch (BeanCreationException e) {
            return new ResponseEntity<>("Bean creation had an error. Job halted.", HttpStatus.BAD_REQUEST);
        } catch (NoSuchElementException e) {
            return new ResponseEntity<>("Requested source doesn't exist", HttpStatus.BAD_REQUEST);
        } catch (JobExecutionAlreadyRunningException e) {
            return new ResponseEntity<>("Job execution already running", HttpStatus.BAD_REQUEST);
        } catch (JobRestartException e) {
            return new ResponseEntity<>("Job restart exception", HttpStatus.BAD_REQUEST);
        } catch (JobInstanceAlreadyCompleteException e) {
            return new ResponseEntity<>("Job already completed", HttpStatus.BAD_REQUEST);
        } catch (JobParametersInvalidException e) {
            return new ResponseEntity<>("Job parameters are invalid", HttpStatus.BAD_REQUEST);
        }

        // Job successfully ran
        return new ResponseEntity<>("Job parameters OK. Job Completed", HttpStatus.CREATED);

    }

    // ----------------------------------------------------------------------------------

    // get unique count
    public ResponseEntity<String> getUniqueCount(String pathInput) {

        try {
            JobParameters jobParameters = buildJobParameters_UniqueCount(pathInput);
            jobLauncher.run(batchConfigUniqueCount.job_getUniqueCount(), jobParameters);

        } catch (BeanCreationException e) {
            return new ResponseEntity<>("Bean creation had an error. Job halted.", HttpStatus.BAD_REQUEST);
        } catch (NoSuchElementException e) {
            return new ResponseEntity<>("Requested source doesn't exist", HttpStatus.BAD_REQUEST);
        } catch (JobExecutionAlreadyRunningException e) {
            return new ResponseEntity<>("Job execution already running", HttpStatus.BAD_REQUEST);
        } catch (JobRestartException e) {
            return new ResponseEntity<>("Job restart exception", HttpStatus.BAD_REQUEST);
        } catch (JobInstanceAlreadyCompleteException e) {
            return new ResponseEntity<>("Job already completed", HttpStatus.BAD_REQUEST);
        } catch (JobParametersInvalidException e) {
            return new ResponseEntity<>("Job parameters are invalid", HttpStatus.BAD_REQUEST);
        }

        // Job successfully ran
        return new ResponseEntity<>("Job parameters OK. Job Completed", HttpStatus.CREATED);
    }
}
