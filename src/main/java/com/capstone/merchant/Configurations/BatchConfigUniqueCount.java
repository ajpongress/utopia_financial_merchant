package com.capstone.merchant.Configurations;

import com.capstone.merchant.Controllers.MerchantController;
import com.capstone.merchant.Listeners.CustomChunkListener;
import com.capstone.merchant.Models.MerchantModel;
import com.capstone.merchant.PathHandlers.ReportsPathHandler;
import com.capstone.merchant.Processors.MerchantProcessor;
import com.capstone.merchant.Processors.UniqueCountProcessor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.util.FileUtil;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.item.support.SynchronizedItemStreamWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.bind.annotation.ResponseStatus;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.InvalidPathException;

@Configuration
@Slf4j
public class BatchConfigUniqueCount {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    @Qualifier("reader_Merchant")
    private SynchronizedItemStreamReader<MerchantModel> synchronizedItemStreamReader;

    @Autowired
    private UniqueCountProcessor uniqueCountProcessor;

    @Autowired
    @Qualifier("writer_Merchant")
    private SynchronizedItemStreamWriter<MerchantModel> xmlWriter;

    @Autowired
    private TaskExecutor taskExecutor;

    @Autowired
    private ReportsPathHandler reportsPathHandler;

    // ----------------------------------------------------------------------------------
    // --                             STEPS & JOBS                                     --
    // ----------------------------------------------------------------------------------

    // Step - unique count
    @Bean
    public Step step_getUniqueCount() {

        return new StepBuilder("getUniqueCountStep", jobRepository)
                .<MerchantModel, MerchantModel> chunk(50000, transactionManager)
                .reader(synchronizedItemStreamReader)
                .processor(uniqueCountProcessor)
                .writer(xmlWriter)
                .listener(new CustomChunkListener())
                .listener(new StepExecutionListener() {

                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {

                        // Get total unique merchants using idCounter from processor
                        long totalUniqueMerchants = uniqueCountProcessor.getIdCounter();

                        // Create reports file using reports file path from Controller API call
//                        String filePath = MerchantController.getReportsPath();
//                        File uniqueMerchantsReport = new File(filePath);
                        File uniqueMerchantsReport = new File(reportsPathHandler.getReportsPath());

                        // Write relevant data to reports file
                        try {
                            BufferedWriter writer = new BufferedWriter(new FileWriter(uniqueMerchantsReport));
                            writer.write("Total unique merchants = " + totalUniqueMerchants);
                            writer.close();

                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        log.info("------------------------------------------------------------------");
                        log.info("Total unique merchants = " + totalUniqueMerchants);
                        log.info("------------------------------------------------------------------");
                        log.info(stepExecution.getSummary());
                        log.info("------------------------------------------------------------------");

                        uniqueCountProcessor.clearAllTrackersAndCounters();

                        return StepExecutionListener.super.afterStep(stepExecution);
                    }
                })
                .taskExecutor(taskExecutor)
                .build();
    }

    // Job - get unique count
    @Bean
    public Job job_getUniqueCount() {

        return new JobBuilder("getUniqueCountJob", jobRepository)
                .start(step_getUniqueCount())
                .build();
    }
}
