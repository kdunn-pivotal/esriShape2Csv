package io.pivotal.pde;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.serialization.StringSerializer;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.Bindings;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.integration.annotation.Splitter;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.integration.kafka.support.KafkaProducerContext;
import org.springframework.integration.kafka.support.ProducerConfiguration;
import org.springframework.integration.kafka.support.ProducerFactoryBean;
import org.springframework.integration.kafka.support.ProducerMetadata;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ByteOrderValues;
import com.vividsolutions.jts.io.WKBWriter;

@EnableBinding(Processor.class)
@SpringBootApplication
public class Shp2CsvProcessor {

    private static Logger LOG = LoggerFactory.getLogger(Shp2CsvProcessor.class);
    private static final String NAME = "Shp2CsvProcessor";
    
    @Autowired
    @Bindings(Shp2CsvProcessor.class)
    private Processor channels;
 
    private MessageChannel output;
 
    @Autowired
    public void SendingBean(MessageChannel output) {
        this.output = output;
    }
    
    @Splitter(inputChannel = Processor.INPUT)
    public void transform(File f) throws Exception {
        // Note, the .dbf needs to be accessible in
        // the same path for the features to be found

        String columnDelimiter = "|";
        String rowDelimiter = "\n";

        Map<String, Object> map = new HashMap<String, Object>();
        URL theUrl = f.toURI().toURL();
        map.put("url", theUrl);
        
        LOG.info("Resolved URL : {}", theUrl);

        DataStore dataStore = DataStoreFinder.getDataStore(map);
        
        if (dataStore == null) {
            LOG.error("DataStore not created for url : {}", theUrl);
            Iterator<DataStoreFactorySpi> availableStores =  DataStoreFinder.getAvailableDataStores();
            LOG.info("List available Stores: {");
                        while (availableStores.hasNext()) {
                            LOG.info(availableStores.next().toString());
                        }
            LOG.info("}");
            throw new Exception();
        }
        
        String typeName = dataStore.getTypeNames()[0];
        LOG.info("DataStore Mapper : Status={}", "DONE");

        FeatureSource<SimpleFeatureType, SimpleFeature> source = dataStore.getFeatureSource(typeName);
        FeatureCollection<SimpleFeatureType, SimpleFeature> collection = source.getFeatures();
        LOG.info("Features Mapped : Status={}", "DONE");
        FeatureIterator<SimpleFeature> features = collection.features();

        WKBWriter wkbWriter; 

        try {
            while(features.hasNext()){
                SimpleFeature feature = features.next();

                List<String> rowStringList = new ArrayList<String>();

                Boolean isFirst = true;
                for (Object thisAttr : feature.getAttributes()) {
                    // Skip the first attribute (geometry column)
                    // it is moved to the end of the row string
                    if (isFirst) {
                        isFirst = false;
                        continue;
                    }
                    rowStringList.add(thisAttr.toString());
                }

                Geometry geom = (Geometry) feature.getDefaultGeometry();
                Integer dimensions = geom.getDimension();
                // the WKB writer needs at least 2 dimensions
                // MULTILINESTRINGS are realistically 2D anyway
                if (dimensions == 1) {
                    dimensions = 2;
                }
                //LOG.info("Dimensions {} ", geom.getDimension());

                wkbWriter = new WKBWriter(dimensions,  ByteOrderValues.LITTLE_ENDIAN); 
                String wkbHexString = WKBWriter.toHex(wkbWriter.write(geom));

                rowStringList.add(wkbHexString);

                // Convert the column list into a delimited string
                String thisRow = StringUtils.join(rowStringList, columnDelimiter) + rowDelimiter;
                
                output.send(MessageBuilder.withPayload(thisRow).build());
            }

        } catch (Exception e) {
            LOG.error("Exception in transformer={} ; Exception={}", NAME, e);
            throw new Exception();
        } finally {
            features.close();
            dataStore.dispose();
        }

        LOG.info("Transformed by {} : Status={}", NAME, "DONE");
        LOG.info("Status={}", "SUCCESS");
    }

    public static void main(String[] args) {
        SpringApplication.run(Shp2CsvProcessor.class, args);
    }
}
