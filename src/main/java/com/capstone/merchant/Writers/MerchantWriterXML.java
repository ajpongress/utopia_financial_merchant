package com.capstone.merchant.Writers;

import com.capstone.merchant.Models.MerchantModel;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.support.SynchronizedItemStreamWriter;
import org.springframework.batch.item.support.builder.SynchronizedItemStreamWriterBuilder;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.batch.item.xml.builder.StaxEventItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.oxm.xstream.XStreamMarshaller;
import org.springframework.stereotype.Component;

import java.util.Collections;

@StepScope
@Component
public class MerchantWriterXML {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    // Destination path for export file
    @Value("#{jobParameters['outputPath_param']}")
    private String outputPath;



    // ----------------------------------------------------------------------------------
    // --                                METHODS                                       --
    // ----------------------------------------------------------------------------------

    // XML Writer
    @StepScope
    @Bean("writer_Merchant")
    public SynchronizedItemStreamWriter<MerchantModel> xmlWriter() {

        XStreamMarshaller marshaller = new XStreamMarshaller();
        marshaller.setAliases(Collections.singletonMap("state", MerchantModel.class));

        StaxEventItemWriter<MerchantModel> writer = new StaxEventItemWriterBuilder<MerchantModel>()
                .name("merchantXmlWriter")
                .resource(new FileSystemResource(outputPath + "/merchant_list.xml"))
                .marshaller(marshaller)
                .rootTagName("merchants")
                .build();

        // Make XML writer thread-safe
        SynchronizedItemStreamWriter<MerchantModel> synchronizedItemStreamWriter =
                new SynchronizedItemStreamWriterBuilder<MerchantModel>()
                        .delegate(writer)
                        .build();
        return synchronizedItemStreamWriter;
    }
}
