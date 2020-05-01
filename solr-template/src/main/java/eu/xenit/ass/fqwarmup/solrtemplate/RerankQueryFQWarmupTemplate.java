package eu.xenit.ass.fqwarmup.solrtemplate;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;

public class RerankQueryFQWarmupTemplate {

    public static void main(String[] args) throws IOException {
        String inputDirectory = args[0];
        String outputDirectory = args[1];

        Path inputPath = Paths.get(inputDirectory);
        Path outputPath = Paths.get(outputDirectory);
        Path solrConfigPath = Paths.get("conf", "solrconfig.xml");
        Path inputXml = inputPath.resolve(solrConfigPath);
        Path outputXml = outputPath.resolve(solrConfigPath);

        FileUtils.deleteDirectory(outputPath.toFile());
        FileUtils.copyDirectory(inputPath.toFile(), outputPath.toFile());

        InputStream in = new FileInputStream(inputXml.toFile());
        OutputStream out = new FileOutputStream(outputXml.toFile());

        WarmupListenerXmlTransformer warmupListenerXmlTransformer = new WarmupListenerXmlTransformer();
        warmupListenerXmlTransformer.transForm(in, out);
    }

}
